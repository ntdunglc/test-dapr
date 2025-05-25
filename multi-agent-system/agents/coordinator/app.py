from fastapi import FastAPI, WebSocket, HTTPException, Depends, Form, status
from contextlib import asynccontextmanager # Added
from dapr.aio.clients import DaprClient
from dapr.ext.fastapi import DaprApp
from pydantic import BaseModel, Field
from typing import Dict, List, Optional, Any
import uuid
import json
import asyncio
from datetime import datetime

from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from jose import jwt, JWTError

# Global Dapr client, to be initialized in lifespan
dapr_client: DaprClient = None # type: ignore

@asynccontextmanager
async def lifespan(app: FastAPI):
    global dapr_client
    dapr_client = DaprClient()
    print("Coordinator DaprClient initialized in lifespan")
    yield
    if dapr_client:
        print("Coordinator Closing DaprClient in lifespan")
        await dapr_client.close()
    dapr_client = None # type: ignore

app = FastAPI(lifespan=lifespan)
dapr_app = DaprApp(app)


# JWT Configuration
SECRET_KEY = "your-secret-key-please-change-in-production"
ALGORITHM = "HS256"
security = HTTPBearer()

# Models
class Agent(BaseModel):
    id: str
    name: str
    type: str
    status: str = "idle"
    last_heartbeat: Optional[datetime] = None
    supported_commands: List[str] = []

class Job(BaseModel):
    id: str
    agent_id: Optional[str]
    status: str = "pending"
    task_type: str
    payload: dict
    created_at: datetime
    completed_at: Optional[datetime] = None
    result: Optional[dict] = None

class Session(BaseModel):
    id: str
    user_id: Optional[str] = "anonymous" # Made user_id optional with a default
    active_agent_id: Optional[str] = None # Agent primarily responsible for this session
    agents: List[str] = [] # Could be used for other participants, or deprecated if active_agent_id is primary
    created_at: datetime
    last_activity: datetime

class CreateSessionRequest(BaseModel):
    user_id: Optional[str] = "anonymous"
    agent_id: Optional[str] = None # To specify the active agent for the new session

# WebSocket connections
websocket_connections: Dict[str, WebSocket] = {}

# In-memory cache for registered agents
# Note: This cache is not persistent across coordinator restarts.
# For persistence, a more robust solution would involve querying/indexing the state store.
registered_agents_cache: Dict[str, Agent] = {}

ALL_SESSIONS_LIST_KEY = "_internal_all_session_ids"
ALL_JOBS_LIST_KEY = "_internal_all_job_ids"


# Custom TopicEvent model to make 'route' field optional
class CustomTopicEvent(BaseModel):
    pubsub_name: str = Field(alias="pubsubname")
    topic: str
    route: Optional[str] = None  # Made optional
    id: str
    data_content_type: str = Field(alias="datacontenttype")
    data: Any
    spec_version: str = Field(alias="specversion")
    type: str
    source: str
    trace_id: Optional[str] = Field(default=None, alias="traceid")
    trace_state: Optional[str] = Field(default=None, alias="tracestate")
    # For backwards compatibility with Dapr 1.2.0 and SDK TopicEvent model
    Data: Optional[Any] = Field(default=None, alias="Data")
    DataContentType: Optional[str] = Field(default=None, alias="DataContentType")
    Id: Optional[str] = Field(default=None, alias="Id")
    PubsubName: Optional[str] = Field(default=None, alias="PubsubName")
    Source: Optional[str] = Field(default=None, alias="Source")
    SpecVersion: Optional[str] = Field(default=None, alias="SpecVersion")
    Topic: Optional[str] = Field(default=None, alias="Topic")
    Type: Optional[str] = Field(default=None, alias="Type")


# Agent Registry
@app.post("/agents/register")
async def register_agent(agent: Agent):
    """Register a new agent"""
    agent.last_heartbeat = datetime.now()
    
    # Save agent state
    await dapr_client.save_state(
        store_name="statestore",
        key=f"agent-{agent.id}",
        value=agent.model_dump_json()
    )
    
    # Publish agent registration event
    await dapr_client.publish_event(
        pubsub_name="pubsub",
        topic_name="agent-events",
        data=json.dumps({ # Serialize to JSON string
            "event": "agent_registered",
            "agent": agent.model_dump(mode='json')
        }),
        data_content_type="application/json" # Specify content type
    )

    # Add to in-memory cache first
    registered_agents_cache[agent.id] = agent
    print(f"Coordinator: Agent {agent.id} registered and added to cache. Cache now: {list(registered_agents_cache.keys())}")

    # Notify WebSocket clients about the new agent
    await broadcast_agent_update(agent.model_dump(mode='json'))
    
    return {"message": "Agent registered successfully", "agent_id": agent.id}

@app.get("/api/agents", response_model=List[Agent]) # Changed path to /api/agents
async def get_registered_agents():
    """Get a list of currently registered agents (from in-memory cache)."""
    print(f"Coordinator: GET /api/agents called. Cache contains: {list(registered_agents_cache.keys())}")
    return list(registered_agents_cache.values())

# Session Management
@app.post("/sessions/create", response_model=Session) # Ensure response_model is Session
async def create_session(request_data: CreateSessionRequest): # Use the request model
    """Create a new user session"""
    session_id = str(uuid.uuid4())
    session = Session(
        id=session_id,
        user_id=request_data.user_id,
        active_agent_id=request_data.agent_id, # Set active agent from request
        created_at=datetime.now(),
        last_activity=datetime.now()
    )
    
    # Save session state (optional, as chat agent manages conversation state)
    # For now, let's save it to have a record of created sessions by the coordinator
    await dapr_client.save_state(
        store_name="statestore",
        key=f"coordinator-session-{session.id}", # Prefixed to distinguish from chat agent's conversation state
        value=session.model_dump_json()
    )

    # Add session ID to the global list of sessions
    try:
        session_ids_state = await dapr_client.get_state(store_name="statestore", key=ALL_SESSIONS_LIST_KEY)
        session_ids = json.loads(session_ids_state.data) if session_ids_state.data else []
    except Exception as e:
        print(f"Coordinator: Error fetching session ID list: {e}. Initializing new list.")
        session_ids = []
    
    if session.id not in session_ids:
        session_ids.append(session.id)
        await dapr_client.save_state(store_name="statestore", key=ALL_SESSIONS_LIST_KEY, value=json.dumps(session_ids))
        print(f"Coordinator: Added session {session.id} to global list. Total sessions: {len(session_ids)}")

    return session # Return the full session object

@app.get("/api/sessions", response_model=List[Session])
async def get_sessions():
    """List all chat sessions known to the coordinator."""
    sessions_list = []
    try:
        session_ids_state = await dapr_client.get_state(store_name="statestore", key=ALL_SESSIONS_LIST_KEY)
        session_ids = json.loads(session_ids_state.data) if session_ids_state.data else []
        
        print(f"Coordinator: Fetched session ID list for /api/sessions. Found {len(session_ids)} IDs: {session_ids}")

        for session_id in session_ids:
            session_state = await dapr_client.get_state(store_name="statestore", key=f"coordinator-session-{session_id}")
            if session_state.data:
                try:
                    session_data = json.loads(session_state.data)
                    sessions_list.append(Session(**session_data))
                except Exception as e:
                    print(f"Coordinator: Error parsing session data for ID {session_id}: {e}")
            else:
                print(f"Coordinator: No session data found for ID {session_id} listed in global list.")
                # Optionally, clean up this ID from ALL_SESSIONS_LIST_KEY if it's stale

    except Exception as e:
        print(f"Coordinator: Error fetching or processing session list for /api/sessions: {e}")
        # Return empty list on error or if the list key doesn't exist
    
    print(f"Coordinator: Returning {len(sessions_list)} sessions for /api/sessions.")
    return sessions_list


# Job Management
@app.post("/jobs/submit")
async def submit_job(
    task_type: str = Form(...),
    description: str = Form(...),
    agent_id: Optional[str] = Form(None)
):
    """Submit a new job to the queue.
    Can be assigned to a specific agent or be general.
    """
    job_id = str(uuid.uuid4())
    job_payload = {"description": description} # Standardize payload for user tasks

    job = Job(
        id=job_id,
        agent_id=agent_id, # Assign agent if specified
        status="pending",
        task_type=task_type, # e.g., "user_task"
        payload=job_payload,
        created_at=datetime.now()
    )
    
    # Save job state
    await dapr_client.save_state(
        store_name="statestore",
        key=f"job-{job.id}",
        value=job.model_dump_json()
    )
    
    # Publish job to queue
    await dapr_client.publish_event(
        pubsub_name="pubsub",
        topic_name="job-queue",
        data=json.dumps(job.model_dump(mode='json')), # Serialize to JSON string
        data_content_type="application/json" # Specify content type
    )
    
    # Notify WebSocket clients
    await broadcast_job_update(job.model_dump(mode='json'))

    # Add job ID to the global list of jobs
    try:
        job_ids_state = await dapr_client.get_state(store_name="statestore", key=ALL_JOBS_LIST_KEY)
        job_ids = json.loads(job_ids_state.data) if job_ids_state.data else []
    except Exception as e:
        print(f"Coordinator: Error fetching job ID list: {e}. Initializing new list.")
        job_ids = []
    
    if job.id not in job_ids:
        job_ids.append(job.id)
        await dapr_client.save_state(store_name="statestore", key=ALL_JOBS_LIST_KEY, value=json.dumps(job_ids))
        print(f"Coordinator: Added job {job.id} to global list. Total jobs in list: {len(job_ids)}")
    
    return {"job_id": job.id}

@app.get("/api/jobs", response_model=List[Job])
async def get_jobs():
    """List all jobs known to the coordinator."""
    jobs_list = []
    try:
        job_ids_state = await dapr_client.get_state(store_name="statestore", key=ALL_JOBS_LIST_KEY)
        job_ids = json.loads(job_ids_state.data) if job_ids_state.data else []
        
        print(f"Coordinator: Fetched job ID list for /api/jobs. Found {len(job_ids)} IDs.")

        for job_id in job_ids:
            job_state = await dapr_client.get_state(store_name="statestore", key=f"job-{job_id}")
            if job_state.data:
                try:
                    job_data = json.loads(job_state.data)
                    jobs_list.append(Job(**job_data))
                except Exception as e:
                    print(f"Coordinator: Error parsing job data for ID {job_id}: {e}")
            else:
                print(f"Coordinator: No job data found for ID {job_id} listed in global list.")
                # Optionally, clean up this ID from ALL_JOBS_LIST_KEY if it's stale

    except Exception as e:
        print(f"Coordinator: Error fetching or processing job list for /api/jobs: {e}")
    
    # Sort jobs by creation date, newest first
    jobs_list.sort(key=lambda j: j.created_at, reverse=True)
    print(f"Coordinator: Returning {len(jobs_list)} jobs for /api/jobs.")
    return jobs_list

async def broadcast_agent_update(agent_data: dict):
    """Broadcast agent updates to all connected clients"""
    message = {
        "type": "agent_update",
        "data": agent_data
    }
    
    for client_id, websocket in websocket_connections.items():
        try:
            print(f"Coordinator: Broadcasting agent_update for agent {agent_data.get('id')} to client {client_id}")
            await websocket.send_json(message)
        except Exception as e:
            print(f"Coordinator: Error sending agent_update to client {client_id}: {e}")
            # Handle potential errors during send, e.g., client disconnected
            pass # Keep pass to avoid breaking connection list iteration

@app.get("/jobs/{job_id}")
async def get_job_status(job_id: str):
    """Get job status"""
    state = await dapr_client.get_state(
        store_name="statestore",
        key=f"job-{job_id}"
    )
    
    if not state.data:
        raise HTTPException(status_code=404, detail="Job not found")
    
    return json.loads(state.data)

# WebSocket for real-time updates
@app.websocket("/ws/{client_id}")
async def websocket_endpoint(websocket: WebSocket, client_id: str):
    await websocket.accept()
    websocket_connections[client_id] = websocket
    
    try:
        while True:
            data = await websocket.receive_text()
            # Handle incoming WebSocket messages
            message = json.loads(data)
            
            if message["type"] == "chat":
                # Expect session_id in chat messages from client
                session_id = message.get("session_id")
                content = message.get("content")
                if session_id and content:
                    await publish_chat_message(sender_id=client_id, content=content, session_id=session_id)
                else:
                    print(f"Coordinator: Received chat message without session_id or content from {client_id}")
    except Exception as e:
        print(f"Coordinator: WebSocket error for client {client_id}: {e}")
        pass
    finally:
        del websocket_connections[client_id]

async def broadcast_job_update(job_data: dict):
    """Broadcast job updates to all connected clients"""
    message = {
        "type": "job_update",
        "data": job_data
    }
    
    for client_id, websocket in websocket_connections.items():
        try:
            await websocket.send_json(message)
        except:
            # Handle potential errors during send, e.g., client disconnected
            pass

async def broadcast_chat_message_to_clients(chat_message_data: dict):
    """Broadcasts a chat message to all connected WebSocket clients."""
    message_to_send = {
        "type": "chat_message",
        "data": chat_message_data
    }
    for client_id, websocket in websocket_connections.items():
        try:
            await websocket.send_json(message_to_send)
        except Exception as e:
            print(f"Error sending chat message to client {client_id}: {e}")
            # Potentially remove dead connections from websocket_connections here

async def publish_chat_message(sender_id: str, content: str, session_id: str): # Added session_id
    """Publish chat message to all agents, including session_id"""
    message = {
        "sender_id": sender_id,
        "content": content,
        "timestamp": datetime.now().isoformat(),
        "session_id": session_id
    }

    # Fetch session details to include active_agent_id if present
    try:
        session_state = await dapr_client.get_state(store_name="statestore", key=f"coordinator-session-{session_id}")
        if session_state.data:
            session_data = json.loads(session_state.data)
            if session_data.get("active_agent_id"):
                message["active_agent_id"] = session_data["active_agent_id"]
                print(f"Coordinator: Attaching active_agent_id {session_data['active_agent_id']} to message for session {session_id}")
    except Exception as e:
        print(f"Coordinator: Error fetching session {session_id} to attach active_agent_id: {e}")
    
    print(f"Coordinator: Publishing chat message to Dapr: {message}")
    await dapr_client.publish_event(
        pubsub_name="pubsub",
        topic_name="chat-messages",
        data=json.dumps(message), # Serialize to JSON string
        data_content_type="application/json" # Specify content type
    )

# Subscribe to events
@dapr_app.subscribe(pubsub="pubsub", topic="job-completed")
async def handle_job_completed(event: CustomTopicEvent): # Use CustomTopicEvent
    """Handle job completion events"""
    print(f"Coordinator Agent: Received job_completed event. Raw event.data type: {type(event.data)}, content_type: {event.data_content_type}")
    job_data = event.data

    if isinstance(job_data, str):
        print(f"Coordinator Agent: event.data for job_completed is a string. Attempting json.loads on: {repr(job_data)}")
        try:
            job_data = json.loads(job_data)
            print(f"Coordinator Agent: Successfully parsed string event.data for job_completed. New type: {type(job_data)}")
        except json.JSONDecodeError as e:
            print(f"Coordinator Agent: Failed to decode JSON from string event.data for job_completed: {e}. Original data: {repr(event.data)}")
            return {"status": "DROP", "error": "event.data string for job_completed is not valid JSON"}
    elif not isinstance(job_data, dict):
        print(f"Coordinator Agent: event.data for job_completed is neither a string nor a dict. Type: {type(job_data)}. Value: {repr(job_data)}")
        return {"status": "DROP", "error": "event.data for job_completed has unexpected type"}

    # Update job state
    try:
        job_id = job_data['id']
    except KeyError:
        print(f"Coordinator Agent: 'id' key missing in job_completed data. Data: {repr(job_data)}")
        return {"status": "DROP", "error": "missing 'id' in job_completed data"}
    except TypeError:
        print(f"Coordinator Agent: job_completed data is not a dictionary. Data: {repr(job_data)}")
        return {"status": "DROP", "error": "job_completed data not a dictionary"}

    await dapr_client.save_state(
        store_name="statestore",
        key=f"job-{job_id}",
        value=json.dumps(job_data)
    )
    
    # Broadcast update
    await broadcast_job_update(job_data)

@dapr_app.subscribe(pubsub="pubsub", topic="chat-messages")
async def handle_incoming_chat_message(event: CustomTopicEvent): # Use CustomTopicEvent
    """Handle incoming chat messages from pub/sub and broadcast to WebSocket clients."""
    print(f"Coordinator Agent: Received chat event. Raw event.data type: {type(event.data)}, content_type: {event.data_content_type}")
    chat_data = event.data

    if isinstance(chat_data, str):
        print(f"Coordinator Agent: event.data for chat is a string. Attempting json.loads on: {repr(chat_data)}")
        try:
            chat_data = json.loads(chat_data)
            print(f"Coordinator Agent: Successfully parsed string event.data for chat. New type: {type(chat_data)}")
        except json.JSONDecodeError as e:
            print(f"Coordinator Agent: Failed to decode JSON from string event.data for chat: {e}. Original data: {repr(event.data)}")
            return {"status": "DROP", "error": "event.data string for chat is not valid JSON"}
    elif not isinstance(chat_data, dict):
        print(f"Coordinator Agent: event.data for chat is neither a string nor a dict. Type: {type(chat_data)}. Value: {repr(chat_data)}")
        return {"status": "DROP", "error": "event.data for chat has unexpected type"}
    
    # Ensure chat_data is a dict before trying to use it for broadcast
    if not isinstance(chat_data, dict):
        # This case should ideally be caught by the checks above, but as a safeguard:
        print(f"Coordinator Agent: chat_data is not a dict after parsing attempts. Cannot broadcast. Data: {repr(chat_data)}")
        return {"status": "DROP", "error": "processed chat_data is not a dictionary"}

    print(f"Coordinator received chat message from pub/sub: {chat_data}")
    await broadcast_chat_message_to_clients(chat_data) # Expects chat_data to be a dict

# Authentication Endpoints
@app.post("/auth/login")
async def login(username: str = Form(...), password: str = Form(...)):
    """Authenticate user and return a JWT token."""
    # In a real application, you would verify username and password against a database.
    # This is a dummy verification for example purposes.
    if not (username == "testuser" and password == "testpass"):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    to_encode = {"sub": username}
    # Ensure the key is bytes for JWT operations
    secret_key_bytes = SECRET_KEY.encode('utf-8')
    encoded_jwt = jwt.encode(to_encode, secret_key_bytes, algorithm=ALGORITHM)
    return {"access_token": encoded_jwt, "token_type": "bearer"}

@app.get("/protected")
async def protected_route(credentials: HTTPAuthorizationCredentials = Depends(security)):
    """A protected route that requires JWT authentication."""
    token = credentials.credentials
    try:
        # Ensure the key is bytes for JWT operations
        secret_key_bytes = SECRET_KEY.encode('utf-8')
        payload = jwt.decode(token, secret_key_bytes, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Could not validate credentials - no username in token",
                headers={"WWW-Authenticate": "Bearer"},
            )
        # Token is valid, username is extracted.
        return {"message": f"Hello {username}! This is a protected route.", "token_payload": payload}
    except JWTError as e:
        error_type_name = type(e).__name__
        error_message = str(e)
        print(f"JWTError encountered in /protected route: {error_type_name} - {error_message}") # Log to coordinator console
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"Could not validate credentials - token error: {error_type_name}", # Include error type in response
            headers={"WWW-Authenticate": "Bearer"},
        )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

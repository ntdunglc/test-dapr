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
    agents: List[str] = []
    created_at: datetime
    last_activity: datetime

class CreateSessionRequest(BaseModel):
    user_id: Optional[str] = "anonymous"

# WebSocket connections
websocket_connections: Dict[str, WebSocket] = {}

# In-memory cache for registered agents
# Note: This cache is not persistent across coordinator restarts.
# For persistence, a more robust solution would involve querying/indexing the state store.
registered_agents_cache: Dict[str, Agent] = {}


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
        user_id=request_data.user_id, # Use user_id from request body
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
    
    return session # Return the full session object

@app.get("/api/sessions")
async def get_sessions():
    """List all chat session IDs by looking at chat agent's conversation states."""
    # This queries for keys created by the chat agent: conversation-<session_id>
    # A more robust way might involve the chat agent explicitly registering sessions,
    # or the coordinator maintaining its own list of session IDs it has created.
    # For now, we infer from chat agent's state.
    query = {
        "filter": {
            "EQ": {"key": "conversation-*"} # This filter might not be supported by all state stores or Dapr query APIs directly.
                                         # A simpler approach is to list all keys and filter client-side,
                                         # or use a specific query if supported.
                                         # Dapr Python SDK get_bulk_state doesn't support wildcard keys.
                                         # We'll have to rely on the coordinator's own created sessions for now,
                                         # or assume the chat agent creates a 'conversation-global' if no session is active.
                                         # Let's list sessions created by the coordinator for now.
        }
    }
    # Due to Dapr state query limitations for general key patterns without specific query capabilities in SDK,
    # we will list sessions that the coordinator itself has created and stored.
    # This means only sessions explicitly created via /sessions/create will be listed.
    # A more advanced solution would be needed for a truly comprehensive list from chat-agent state.

    # For simplicity, let's assume we list sessions the coordinator has created.
    # This requires storing session IDs in a list or querying for "coordinator-session-*"
    # Let's refine this to query for "coordinator-session-*" keys.
    # However, Dapr SDK's get_bulk_state doesn't support wildcard key fetching.
    # And query_state is for specific query languages (e.g. JSON query for Redis).

    # Simplification: We will return sessions from the coordinator's in-memory cache of created sessions
    # This is not robust across coordinator restarts.
    # A better approach would be to store a list of session_ids in the state store.
    # For now, let's return an empty list, as implementing robust session listing from state store
    # without proper query support or a dedicated list is complex.
    # The UI will create sessions and can store them locally.
    # Let's return sessions from the coordinator's state store if we stored them with a specific prefix.
    
    # Given the constraints, the most straightforward way to list sessions created by the coordinator
    # is if we had a dedicated list in the state store. Since we don't,
    # and querying by prefix is not directly supported by get_bulk_state,
    # this endpoint will be hard to implement robustly without further changes to how sessions are tracked.

    # Let's assume for now that the UI will manage its known sessions,
    # and this endpoint can be a placeholder or enhanced later.
    # For a first pass, we can return sessions stored by the coordinator.
    # This requires iterating through all keys, which is not efficient or directly supported.

    # Fallback: Return an empty list. The UI will create sessions and can manage them.
    # This part needs a more robust design for production.
    # For this exercise, we'll return an empty list and let the UI drive session creation and local tracking.
    # The chat agent will still store history per session_id it receives.
    # The UI will need to remember the session IDs it creates.
    print("Warning: /api/sessions currently returns an empty list. UI should manage its own session IDs.")
    return []


# Job Management
@app.post("/jobs/submit")
async def submit_job(task_type: str, payload: dict):
    """Submit a new job to the queue"""
    job = Job(
        id=str(uuid.uuid4()),
        task_type=task_type,
        payload=payload,
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
    
    return {"job_id": job.id}

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
        "session_id": session_id # Include session_id in the published message
    }
    
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

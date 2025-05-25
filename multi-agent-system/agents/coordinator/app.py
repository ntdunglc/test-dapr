from fastapi import FastAPI, WebSocket, HTTPException, Depends, Form, status
from contextlib import asynccontextmanager # Added
from dapr.aio.clients import DaprClient
from dapr.ext.fastapi import DaprApp
from pydantic import BaseModel
from typing import Dict, List, Optional
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
    user_id: str
    agents: List[str] = []
    created_at: datetime
    last_activity: datetime

# WebSocket connections
websocket_connections: Dict[str, WebSocket] = {}

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
    
    return {"message": "Agent registered successfully", "agent_id": agent.id}

# Session Management
@app.post("/sessions/create")
async def create_session(user_id: str):
    """Create a new user session"""
    session = Session(
        id=str(uuid.uuid4()),
        user_id=user_id,
        created_at=datetime.now(),
        last_activity=datetime.now()
    )
    
    # Save session state
    await dapr_client.save_state(
        store_name="statestore",
        key=f"session-{session.id}",
        value=session.model_dump_json()
    )
    
    return {"session_id": session.id}

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
                # Publish chat message
                await publish_chat_message(client_id, message["content"])
    except:
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

async def publish_chat_message(sender_id: str, content: str):
    """Publish chat message to all agents"""
    message = {
        "sender_id": sender_id,
        "content": content,
        "timestamp": datetime.now().isoformat()
    }
    
    await dapr_client.publish_event(
        pubsub_name="pubsub",
        topic_name="chat-messages",
        data=json.dumps(message), # Serialize to JSON string
        data_content_type="application/json" # Specify content type
    )

# Subscribe to events
@dapr_app.subscribe(pubsub="pubsub", topic="job-completed")
async def handle_job_completed(event):
    """Handle job completion events"""
    job_data = event.data
    
    # Update job state
    await dapr_client.save_state(
        store_name="statestore",
        key=f"job-{job_data['id']}",
        value=json.dumps(job_data)
    )
    
    # Broadcast update
    await broadcast_job_update(job_data)

@dapr_app.subscribe(pubsub="pubsub", topic="chat-messages")
async def handle_incoming_chat_message(event):
    """Handle incoming chat messages from pub/sub and broadcast to WebSocket clients."""
    chat_data = event.data # This is already a dict
    print(f"Coordinator received chat message from pub/sub: {chat_data}")
    await broadcast_chat_message_to_clients(chat_data)

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

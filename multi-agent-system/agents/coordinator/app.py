from fastapi import FastAPI, WebSocket, HTTPException
from dapr.clients import DaprClient
from dapr.ext.fastapi import DaprApp
from pydantic import BaseModel
from typing import Dict, List, Optional
import uuid
import json
import asyncio
from datetime import datetime

app = FastAPI()
dapr_app = DaprApp(app)
dapr_client = DaprClient()

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
    dapr_client.save_state(
        store_name="statestore",
        key=f"agent-{agent.id}",
        value=agent.dict()
    )
    
    # Publish agent registration event
    dapr_client.publish_event(
        pubsub_name="pubsub",
        topic_name="agent-events",
        data={
            "event": "agent_registered",
            "agent": agent.dict()
        }
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
    dapr_client.save_state(
        store_name="statestore",
        key=f"session-{session.id}",
        value=session.dict()
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
    dapr_client.save_state(
        store_name="statestore",
        key=f"job-{job.id}",
        value=job.dict()
    )
    
    # Publish job to queue
    dapr_client.publish_event(
        pubsub_name="pubsub",
        topic_name="job-queue",
        data=job.dict()
    )
    
    # Notify WebSocket clients
    await broadcast_job_update(job.dict())
    
    return {"job_id": job.id}

@app.get("/jobs/{job_id}")
async def get_job_status(job_id: str):
    """Get job status"""
    state = dapr_client.get_state(
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
            pass

async def publish_chat_message(sender_id: str, content: str):
    """Publish chat message to all agents"""
    message = {
        "sender_id": sender_id,
        "content": content,
        "timestamp": datetime.now().isoformat()
    }
    
    dapr_client.publish_event(
        pubsub_name="pubsub",
        topic_name="chat-messages",
        data=message
    )

# Subscribe to events
@dapr_app.subscribe(pubsub="pubsub", topic="job-completed")
async def handle_job_completed(event):
    """Handle job completion events"""
    job_data = event.data
    
    # Update job state
    dapr_client.save_state(
        store_name="statestore",
        key=f"job-{job_data['id']}",
        value=job_data
    )
    
    # Broadcast update
    await broadcast_job_update(job_data)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

# Multi-Agent System with Dapr and Python

## Overview

This guide walks you through building a multi-agent system using Dapr with Python, featuring:
- State persistence
- Session management
- Real-time chat with WebSocket
- Background job processing with monitoring
- Web UI with real-time updates

## Prerequisites

1. **Install Dapr CLI**
```bash
# For Linux/macOS
wget -q https://raw.githubusercontent.com/dapr/cli/master/install/install.sh -O - | /bin/bash

# For Windows
powershell -Command "iwr -useb https://raw.githubusercontent.com/dapr/cli/master/install/install.ps1 | iex"
```

2. **Initialize Dapr**
```bash
dapr init
```

3. **Install Python 3.8+** and required packages

## Project Structure

```
multi-agent-system/
├── components/
│   ├── statestore.yaml
│   ├── pubsub.yaml
│   └── bindings.yaml
├── agents/
│   ├── coordinator/
│   │   ├── app.py
│   │   └── requirements.txt
│   ├── worker/
│   │   ├── app.py
│   │   └── requirements.txt
│   └── chat/
│       ├── app.py
│       └── requirements.txt
├── web-ui/
│   ├── static/
│   │   ├── index.html
│   │   ├── style.css
│   │   └── app.js
│   └── server.py
├── dapr.yaml
└── requirements.txt
```

## Step 1: Configure Dapr Components

### State Store Component (`components/statestore.yaml`)
```yaml
apiVersion: dapr.io/v1alpha1
kind: Component
metadata:
  name: statestore
spec:
  type: state.redis
  version: v1
  metadata:
  - name: redisHost
    value: localhost:6379
  - name: redisPassword
    value: ""
  - name: actorStateStore
    value: "true"
```

### Pub/Sub Component (`components/pubsub.yaml`)
```yaml
apiVersion: dapr.io/v1alpha1
kind: Component
metadata:
  name: pubsub
spec:
  type: pubsub.redis
  version: v1
  metadata:
  - name: redisHost
    value: localhost:6379
  - name: redisPassword
    value: ""
```

### Bindings Component for Jobs (`components/bindings.yaml`)
```yaml
apiVersion: dapr.io/v1alpha1
kind: Component
metadata:
  name: jobs-queue
spec:
  type: bindings.redis
  version: v1
  metadata:
  - name: redisHost
    value: localhost:6379
  - name: redisPassword
    value: ""
  - name: enableTLS
    value: false
```

## Step 2: Create the Coordinator Agent

### `agents/coordinator/app.py`
```python
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
```

### `agents/coordinator/requirements.txt`
```
fastapi==0.104.1
uvicorn==0.24.0
dapr==1.12.0
dapr-ext-fastapi==1.12.0
pydantic==2.4.2
websockets==11.0.3
```

## Step 3: Create Worker Agent

### `agents/worker/app.py`
```python
from fastapi import FastAPI
from dapr.clients import DaprClient
from dapr.ext.fastapi import DaprApp
import json
import time
import asyncio
from datetime import datetime

app = FastAPI()
dapr_app = DaprApp(app)
dapr_client = DaprClient()

AGENT_ID = "worker-1"
AGENT_NAME = "Worker Agent 1"

# Register agent on startup
@app.on_event("startup")
async def register_with_coordinator():
    """Register this worker with the coordinator"""
    agent_data = {
        "id": AGENT_ID,
        "name": AGENT_NAME,
        "type": "worker",
        "status": "active"
    }
    
    # Use Dapr service invocation to register
    dapr_client.invoke_method(
        app_id="coordinator",
        method_name="agents/register",
        data=json.dumps(agent_data),
        http_verb="POST"
    )

# Subscribe to job queue
@dapr_app.subscribe(pubsub="pubsub", topic="job-queue")
async def process_job(event):
    """Process incoming jobs"""
    job_data = event.data
    
    print(f"Received job: {job_data['id']}")
    
    # Update job status to processing
    job_data["status"] = "processing"
    job_data["agent_id"] = AGENT_ID
    
    dapr_client.save_state(
        store_name="statestore",
        key=f"job-{job_data['id']}",
        value=job_data
    )
    
    # Simulate job processing
    result = await execute_job(job_data)
    
    # Update job completion
    job_data["status"] = "completed"
    job_data["completed_at"] = datetime.now().isoformat()
    job_data["result"] = result
    
    # Publish completion event
    dapr_client.publish_event(
        pubsub_name="pubsub",
        topic_name="job-completed",
        data=job_data
    )
    
    return {"success": True}

async def execute_job(job_data: dict) -> dict:
    """Execute the actual job based on task type"""
    task_type = job_data.get("task_type")
    payload = job_data.get("payload", {})
    
    if task_type == "data_processing":
        # Simulate data processing
        await asyncio.sleep(5)
        return {
            "processed_items": payload.get("item_count", 0),
            "processing_time": 5
        }
    
    elif task_type == "analysis":
        # Simulate analysis
        await asyncio.sleep(3)
        return {
            "analysis_result": "completed",
            "confidence": 0.95
        }
    
    else:
        return {"error": f"Unknown task type: {task_type}"}

# Subscribe to chat messages
@dapr_app.subscribe(pubsub="pubsub", topic="chat-messages")
async def handle_chat_message(event):
    """Handle incoming chat messages"""
    message = event.data
    print(f"Chat message from {message['sender_id']}: {message['content']}")
    
    # Process message and potentially respond
    if "help" in message["content"].lower():
        response = {
            "sender_id": AGENT_ID,
            "content": f"{AGENT_NAME} here. How can I assist?",
            "timestamp": datetime.now().isoformat()
        }
        
        dapr_client.publish_event(
            pubsub_name="pubsub",
            topic_name="chat-messages",
            data=response
        )

# Heartbeat
async def send_heartbeat():
    """Send periodic heartbeat to coordinator"""
    while True:
        agent_data = {
            "id": AGENT_ID,
            "last_heartbeat": datetime.now().isoformat()
        }
        
        dapr_client.save_state(
            store_name="statestore",
            key=f"agent-{AGENT_ID}",
            value=agent_data,
            state_metadata={"merge": "true"}
        )
        
        await asyncio.sleep(30)

@app.on_event("startup")
async def start_heartbeat():
    asyncio.create_task(send_heartbeat())

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)
```

### `agents/worker/requirements.txt`
```
fastapi==0.104.1
uvicorn==0.24.0
dapr==1.12.0
dapr-ext-fastapi==1.12.0
```

## Step 4: Create Chat Agent

### `agents/chat/app.py`
```python
from fastapi import FastAPI
from dapr.clients import DaprClient
from dapr.ext.fastapi import DaprApp
import json
from datetime import datetime

app = FastAPI()
dapr_app = DaprApp(app)
dapr_client = DaprClient()

AGENT_ID = "chat-agent"
AGENT_NAME = "Chat Coordinator"

# Message history storage
@dapr_app.subscribe(pubsub="pubsub", topic="chat-messages")
async def store_chat_message(event):
    """Store chat messages in state store"""
    message = event.data
    
    # Store message with timestamp key
    timestamp_key = f"chat-{message['timestamp']}"
    dapr_client.save_state(
        store_name="statestore",
        key=timestamp_key,
        value=message
    )
    
    # Update conversation history
    conversation_key = f"conversation-{message.get('session_id', 'global')}"
    
    # Get existing conversation
    state = dapr_client.get_state(
        store_name="statestore",
        key=conversation_key
    )
    
    conversation = json.loads(state.data) if state.data else {"messages": []}
    conversation["messages"].append(message)
    
    # Keep only last 100 messages
    if len(conversation["messages"]) > 100:
        conversation["messages"] = conversation["messages"][-100:]
    
    dapr_client.save_state(
        store_name="statestore",
        key=conversation_key,
        value=conversation
    )
    
    return {"success": True}

@app.get("/chat/history/{session_id}")
async def get_chat_history(session_id: str):
    """Get chat history for a session"""
    conversation_key = f"conversation-{session_id}"
    
    state = dapr_client.get_state(
        store_name="statestore",
        key=conversation_key
    )
    
    if state.data:
        return json.loads(state.data)
    else:
        return {"messages": []}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8002)
```

## Step 5: Create Web UI

### `web-ui/static/index.html`
```html
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Multi-Agent System Dashboard</title>
    <link rel="stylesheet" href="/static/style.css">
</head>
<body>
    <div class="container">
        <h1>Multi-Agent System Dashboard</h1>
        
        <div class="grid">
            <!-- Agent Status -->
            <div class="panel">
                <h2>Agents</h2>
                <div id="agents-list"></div>
            </div>
            
            <!-- Job Queue -->
            <div class="panel">
                <h2>Jobs</h2>
                <button onclick="submitJob()">Submit New Job</button>
                <div id="jobs-list"></div>
            </div>
            
            <!-- Chat Interface -->
            <div class="panel full-width">
                <h2>Agent Chat</h2>
                <div id="chat-messages"></div>
                <div class="chat-input">
                    <input type="text" id="chat-input" placeholder="Type a message...">
                    <button onclick="sendMessage()">Send</button>
                </div>
            </div>
        </div>
    </div>
    
    <script src="/static/app.js"></script>
</body>
</html>
```

### `web-ui/static/style.css`
```css
body {
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif;
    margin: 0;
    padding: 0;
    background-color: #f5f5f5;
}

.container {
    max-width: 1200px;
    margin: 0 auto;
    padding: 20px;
}

h1 {
    color: #333;
}

.grid {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 20px;
}

.panel {
    background: white;
    border-radius: 8px;
    padding: 20px;
    box-shadow: 0 2px 4px rgba(0,0,0,0.1);
}

.panel.full-width {
    grid-column: 1 / -1;
}

#chat-messages {
    height: 300px;
    overflow-y: auto;
    border: 1px solid #eee;
    padding: 10px;
    margin-bottom: 10px;
}

.chat-input {
    display: flex;
    gap: 10px;
}

.chat-input input {
    flex: 1;
    padding: 8px;
    border: 1px solid #ddd;
    border-radius: 4px;
}

button {
    background: #007bff;
    color: white;
    border: none;
    padding: 8px 16px;
    border-radius: 4px;
    cursor: pointer;
}

button:hover {
    background: #0056b3;
}

.job-item, .agent-item {
    padding: 10px;
    border-bottom: 1px solid #eee;
}

.job-item:last-child, .agent-item:last-child {
    border-bottom: none;
}

.status {
    display: inline-block;
    padding: 2px 8px;
    border-radius: 3px;
    font-size: 12px;
}

.status.active { background: #28a745; color: white; }
.status.idle { background: #ffc107; color: #333; }
.status.pending { background: #6c757d; color: white; }
.status.processing { background: #17a2b8; color: white; }
.status.completed { background: #28a745; color: white; }
```

### `web-ui/static/app.js`
```javascript
let ws = null;
const clientId = Math.random().toString(36).substring(7);

// Initialize WebSocket connection
function initWebSocket() {
    ws = new WebSocket(`ws://localhost:8000/ws/${clientId}`);
    
    ws.onopen = () => {
        console.log('Connected to coordinator');
        loadInitialData();
    };
    
    ws.onmessage = (event) => {
        const message = JSON.parse(event.data);
        handleMessage(message);
    };
    
    ws.onclose = () => {
        console.log('Disconnected from coordinator');
        setTimeout(initWebSocket, 3000); // Reconnect after 3 seconds
    };
}

// Handle incoming messages
function handleMessage(message) {
    switch(message.type) {
        case 'job_update':
            updateJobDisplay(message.data);
            break;
        case 'chat_message':
            displayChatMessage(message.data);
            break;
        case 'agent_update':
            updateAgentDisplay(message.data);
            break;
    }
}

// Load initial data
async function loadInitialData() {
    // Load agents
    const agentsResponse = await fetch('/api/agents');
    const agents = await agentsResponse.json();
    agents.forEach(agent => updateAgentDisplay(agent));
    
    // Load recent jobs
    const jobsResponse = await fetch('/api/jobs/recent');
    const jobs = await jobsResponse.json();
    jobs.forEach(job => updateJobDisplay(job));
    
    // Load chat history
    const chatResponse = await fetch('/api/chat/history/global');
    const chatHistory = await chatResponse.json();
    chatHistory.messages.forEach(msg => displayChatMessage(msg));
}

// Submit a new job
async function submitJob() {
    const taskTypes = ['data_processing', 'analysis'];
    const taskType = taskTypes[Math.floor(Math.random() * taskTypes.length)];
    
    const payload = {
        item_count: Math.floor(Math.random() * 1000) + 100,
        priority: Math.random() > 0.5 ? 'high' : 'normal'
    };
    
    const response = await fetch('/api/jobs/submit', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json'
        },
        body: JSON.stringify({
            task_type: taskType,
            payload: payload
        })
    });
    
    const result = await response.json();
    console.log('Job submitted:', result.job_id);
}

// Send chat message
function sendMessage() {
    const input = document.getElementById('chat-input');
    const message = input.value.trim();
    
    if (message && ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({
            type: 'chat',
            content: message
        }));
        input.value = '';
    }
}

// Update displays
function updateJobDisplay(job) {
    const jobsList = document.getElementById('jobs-list');
    let jobElement = document.getElementById(`job-${job.id}`);
    
    if (!jobElement) {
        jobElement = document.createElement('div');
        jobElement.id = `job-${job.id}`;
        jobElement.className = 'job-item';
        jobsList.prepend(jobElement);
    }
    
    jobElement.innerHTML = `
        <div>
            <strong>Job ${job.id.substring(0, 8)}</strong>
            <span class="status ${job.status}">${job.status}</span>
        </div>
        <div>Type: ${job.task_type}</div>
        ${job.agent_id ? `<div>Agent: ${job.agent_id}</div>` : ''}
        ${job.result ? `<div>Result: ${JSON.stringify(job.result)}</div>` : ''}
    `;
}

function updateAgentDisplay(agent) {
    const agentsList = document.getElementById('agents-list');
    let agentElement = document.getElementById(`agent-${agent.id}`);
    
    if (!agentElement) {
        agentElement = document.createElement('div');
        agentElement.id = `agent-${agent.id}`;
        agentElement.className = 'agent-item';
        agentsList.appendChild(agentElement);
    }
    
    agentElement.innerHTML = `
        <div>
            <strong>${agent.name}</strong>
            <span class="status ${agent.status}">${agent.status}</span>
        </div>
        <div>Type: ${agent.type}</div>
    `;
}

function displayChatMessage(message) {
    const chatMessages = document.getElementById('chat-messages');
    const messageElement = document.createElement('div');
    messageElement.innerHTML = `
        <strong>${message.sender_id}:</strong> ${message.content}
        <span style="color: #999; font-size: 12px; margin-left: 10px;">
            ${new Date(message.timestamp).toLocaleTimeString()}
        </span>
    `;
    chatMessages.appendChild(messageElement);
    chatMessages.scrollTop = chatMessages.scrollHeight;
}

// Event listeners
document.getElementById('chat-input').addEventListener('keypress', (e) => {
    if (e.key === 'Enter') {
        sendMessage();
    }
});

// Initialize
initWebSocket();
```

### `web-ui/server.py`
```python
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
import httpx

app = FastAPI()

# Mount static files
app.mount("/static", StaticFiles(directory="static"), name="static")

# Proxy API calls to coordinator
@app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "DELETE"])
async def proxy(path: str, request):
    async with httpx.AsyncClient() as client:
        # Forward request to coordinator
        url = f"http://localhost:50000/{path}"
        
        response = await client.request(
            method=request.method,
            url=url,
            content=await request.body(),
            headers=dict(request.headers)
        )
        
        return response.content

@app.get("/")
async def read_index():
    return FileResponse('static/index.html')

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)
```

## Step 6: Dapr Configuration File

### `dapr.yaml`
```yaml
version: 1
common:
  resourcesPath: ./components/
apps:
  - appID: coordinator
    appDirPath: ./agents/coordinator/
    appPort: 8000
    command: ["python", "app.py"]
    
  - appID: worker
    appDirPath: ./agents/worker/
    appPort: 8001
    command: ["python", "app.py"]
    
  - appID: chat
    appDirPath: ./agents/chat/
    appPort: 8002
    command: ["python", "app.py"]
```

## Step 7: Running the System

1. **Install dependencies for each agent:**
```bash
cd agents/coordinator && pip install -r requirements.txt
cd ../worker && pip install -r requirements.txt
cd ../chat && pip install -r requirements.txt
cd ../../web-ui && pip install fastapi uvicorn httpx
```

2. **Start all services with Dapr:**
```bash
# From the project root directory
dapr run -f .
```

3. **Start the Web UI separately:**
```bash
cd web-ui
python server.py
```

4. **Access the dashboard:**
Open your browser to `http://localhost:8080`

## Features Implemented

### 1. **Persistence**
- State store using Redis for agents, jobs, sessions, and chat history
- Automatic state synchronization across agents

### 2. **State Management**
- Each agent maintains its state in Dapr state store
- Job states transition through: pending → processing → completed
- Agent states include: idle, active, processing

### 3. **Session Management**
- User sessions tracked with unique IDs
- Session state persisted in state store
- Activity tracking for each session

### 4. **Real-time Chat**
- WebSocket connection for real-time updates
- Pub/sub messaging between agents
- Chat history persistence

### 5. **Background Jobs**
- Job queue using pub/sub
- Worker agents process jobs asynchronously
- Real-time job status updates via WebSocket

### 6. **Monitoring UI**
- Live agent status display
- Job queue visualization
- Real-time chat interface
- Automatic reconnection on disconnect

## Advanced Features

### Scaling Workers
```bash
# Run multiple worker instances
dapr run --app-id worker-2 --app-port 8003 --dapr-http-port 3503 -- python agents/worker/app.py
dapr run --app-id worker-3 --app-port 8004 --dapr-http-port 3504 -- python agents/worker/app.py
```

### Adding Authentication
Implement JWT authentication in the coordinator:
```python
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from jose import jwt

security = HTTPBearer()

@app.post("/auth/login")
async def login(username: str, password: str):
    # Verify credentials
    token = jwt.encode({"user_id": username}, SECRET_KEY)
    return {"access_token": token}

@app.get("/protected")
async def protected_route(credentials: HTTPAuthorizationCredentials = Depends(security)):
    token = credentials.credentials
    # Verify token
```

### Monitoring and Observability
Enable Dapr telemetry:
```yaml
# In dapr configuration
spec:
  tracing:
    samplingRate: "1"
    zipkin:
      endpointAddress: "http://localhost:9411/api/v2/spans"
```

## Troubleshooting

1. **Redis connection issues:**
   - Ensure Redis is running: `dapr init` should install it
   - Check Redis port: default is 6379

2. **WebSocket connection fails:**
   - Check CORS settings if running from different domain
   - Ensure coordinator service is running on port 8000

3. **Jobs not processing:**
   - Check worker agent logs
   - Verify pub/sub component configuration

4. **State not persisting:**
   - Check state store component configuration
   - Verify Redis is
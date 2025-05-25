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
    await dapr_client.invoke_method_async(
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
    
    await dapr_client.save_state_async(
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
    await dapr_client.publish_event_async(
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
        
        await dapr_client.publish_event_async(
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
        
        await dapr_client.save_state_async(
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

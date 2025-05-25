from fastapi import FastAPI
from dapr.aio.clients import DaprClient # Changed to async client
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
    await dapr_client.save_state(
        store_name="statestore",
        key=timestamp_key,
        value=json.dumps(message)
    )
    
    # Update conversation history
    conversation_key = f"conversation-{message.get('session_id', 'global')}"
    
    # Get existing conversation
    state = await dapr_client.get_state(
        store_name="statestore",
        key=conversation_key
    )
    
    conversation = json.loads(state.data) if state.data else {"messages": []}
    conversation["messages"].append(message)
    
    # Keep only last 100 messages
    if len(conversation["messages"]) > 100:
        conversation["messages"] = conversation["messages"][-100:]
    
    await dapr_client.save_state(
        store_name="statestore",
        key=conversation_key,
        value=json.dumps(conversation)
    )
    
    return {"success": True}

@app.get("/chat/history/{session_id}")
async def get_chat_history(session_id: str):
    """Get chat history for a session"""
    conversation_key = f"conversation-{session_id}"
    
    state = await dapr_client.get_state(
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

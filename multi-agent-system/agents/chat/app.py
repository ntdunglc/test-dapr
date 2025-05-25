from fastapi import FastAPI
from dapr.aio.clients import DaprClient
from dapr.ext.fastapi import DaprApp
from pydantic import BaseModel, Field # Added Field
from typing import Any, Optional # Added Any, Optional
import json
from datetime import datetime
from contextlib import asynccontextmanager

# Global Dapr client, to be initialized in lifespan
dapr_client: DaprClient = None # type: ignore

@asynccontextmanager
async def lifespan(app: FastAPI):
    global dapr_client
    dapr_client = DaprClient()
    print("Chat DaprClient initialized in lifespan")
    yield
    if dapr_client:
        print("Chat Closing DaprClient in lifespan")
        await dapr_client.close()
    dapr_client = None # type: ignore

app = FastAPI(lifespan=lifespan)
dapr_app = DaprApp(app)

AGENT_ID = "chat-agent"
AGENT_NAME = "Chat Coordinator"


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


# Message history storage
@dapr_app.subscribe(pubsub="pubsub", topic="chat-messages")
async def store_chat_message(event: CustomTopicEvent): # Use CustomTopicEvent
    """Store chat messages in state store"""
    message_data = event.data
    if isinstance(message_data, str) and (event.data_content_type and 'application/json' in event.data_content_type.lower()):
        try:
            message_data = json.loads(message_data)
        except json.JSONDecodeError as e:
            print(f"Chat: Failed to decode JSON message_data: {e}. Data: {event.data}")
            return {"status": "DROP"} # Or RETRY
    
    # Store message with timestamp key
    timestamp_key = f"chat-{message_data['timestamp']}"
    await dapr_client.save_state(
        store_name="statestore",
        key=timestamp_key,
        value=json.dumps(message_data) # Save the (potentially parsed) dict as JSON
    )
    
    # Update conversation history
    conversation_key = f"conversation-{message_data.get('session_id', 'global')}"
    
    # Get existing conversation
    state = await dapr_client.get_state(
        store_name="statestore",
        key=conversation_key
    )
    
    conversation = json.loads(state.data) if state.data else {"messages": []}
    conversation["messages"].append(message_data) # Append the dict
    
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

from fastapi import FastAPI
from dapr.aio.clients import DaprClient
from dapr.ext.fastapi import DaprApp
from pydantic import BaseModel, Field # Added Field
from typing import Any, Optional # Added Any, Optional
import json
import uuid # For default message ID
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
STATE_STORE_NAME = "statestore" # Define state store name for consistency

# Define a richer Pydantic model for chat messages stored by this agent
class AppChatMessage(BaseModel):
    id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    session_id: str # The actual session ID (e.g., UUID from coordinator)
    sender_id: str
    role: str # 'user' or 'assistant' typically for chat, or agent name
    content: str
    timestamp: str = Field(default_factory=lambda: datetime.now().isoformat() + "Z")
    # Add any other fields you need, e.g., active_agent_id if relevant at message level
    active_agent_id: Optional[str] = None
    # Ensure all fields that need to be serialized properly (like datetime) are handled

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
    """Store chat messages in state store as a list of rich message objects."""
    if not dapr_client:
        print("Chat Agent: Dapr client not initialized. Dropping message.")
        return {"status": "DROP", "error": "Dapr client not initialized"}

    print(f"Chat Agent: Received chat event. Raw event.data type: {type(event.data)}, content_type: {event.data_content_type}")
    message_data = event.data

    if isinstance(message_data, str):
        print(f"Chat Agent: event.data is a string. Attempting json.loads on: {repr(message_data)}")
        try:
            message_data = json.loads(message_data)
            print(f"Chat Agent: Successfully parsed string event.data. New type: {type(message_data)}")
        except json.JSONDecodeError as e:
            print(f"Chat Agent: Failed to decode JSON from string event.data: {e}. Original data: {repr(event.data)}")
            return {"status": "DROP", "error": "event.data string is not valid JSON"}
    elif not isinstance(message_data, dict):
        print(f"Chat Agent: event.data is neither a string nor a dict. Type: {type(message_data)}. Value: {repr(message_data)}")
        return {"status": "DROP", "error": "event.data has unexpected type"}
    
    # At this point, message_data should be a dict.
    try:
        actual_session_id = message_data.get("session_id")
        sender_id = message_data.get("sender_id")
        content = message_data.get("content")
        timestamp = message_data.get("timestamp", datetime.now().isoformat() + "Z")
        active_agent_id = message_data.get("active_agent_id")

        if not actual_session_id or not sender_id or content is None:
            print(f"Chat Agent: Missing required fields (session_id, sender_id, content) in message_data: {message_data}")
            return {"status": "DROP", "error": "Missing required fields in message_data"}

        role = "user" 
        if sender_id == AGENT_ID:
            role = "assistant"
        elif sender_id and (not sender_id.startswith("user-") and sender_id != "UI"):
            role = sender_id

        new_message_obj = AppChatMessage(
            session_id=actual_session_id,
            sender_id=sender_id,
            role=role,
            content=content,
            timestamp=timestamp,
            active_agent_id=active_agent_id
        )
        new_message_dict = new_message_obj.model_dump(mode="json")

        conversation_key = f"conversation-{actual_session_id}"
        
        state = await dapr_client.get_state(store_name=STATE_STORE_NAME, key=conversation_key)
        current_messages = []
        if state.data:
            try:
                current_messages = json.loads(state.data)
            except json.JSONDecodeError:
                print(f"Chat Agent: Failed to decode existing state for {conversation_key}. Re-initializing list.")
                current_messages = []
        
        if not isinstance(current_messages, list):
            print(f"Chat Agent: Data for key {conversation_key} is not a list (type: {type(current_messages)}). Re-initializing list.")
            current_messages = []

        current_messages.append(new_message_dict)
        
        if len(current_messages) > 100: # Keep only last 100 messages
            current_messages = current_messages[-100:]
        
        await dapr_client.save_state(
            store_name=STATE_STORE_NAME,
            key=conversation_key,
            value=json.dumps(current_messages)
        )
        print(f"Chat Agent: Stored/updated message list for session {actual_session_id} under key {conversation_key}")
        return {"status": "SUCCESS"}
    except Exception as e:
        print(f"Chat Agent: Error processing and storing chat message: {e}. Data: {message_data}")
        return {"status": "RETRY", "error": str(e)}

@app.get("/chat/history/{session_id}")
async def get_chat_history(session_id: str):
    """Get chat history for a session"""
    if not dapr_client:
        print("Chat Agent: Dapr client not initialized for get_chat_history.")
        return {"messages": [], "error": "Dapr client not initialized"}

    conversation_key = f"conversation-{session_id}"
    
    try:
        state = await dapr_client.get_state(
            store_name=STATE_STORE_NAME,
            key=conversation_key
        )
        
        if state.data:
            messages_list = []
            try:
                messages_list = json.loads(state.data) 
            except json.JSONDecodeError:
                print(f"Chat Agent: Failed to decode state data for {conversation_key} in get_chat_history.")
                return {"messages": [], "error": "Failed to decode chat history"}

            if isinstance(messages_list, list):
                return {"messages": messages_list}
            else:
                print(f"Chat Agent: Chat history for {conversation_key} is not a list (type: {type(messages_list)}).")
                return {"messages": [], "error": "Chat history format error"}
        else:
            return {"messages": []} 
    except Exception as e:
        print(f"Chat Agent: Error retrieving history for session {session_id} (key {conversation_key}): {e}")
        return {"messages": [], "error": str(e)}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8002)

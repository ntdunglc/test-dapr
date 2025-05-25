from fastapi import FastAPI
from dapr.aio.clients import DaprClient
from dapr.ext.fastapi import DaprApp
from pydantic import BaseModel, Field # Added Field
from typing import Any, Optional # Added Any, Optional
import json
import time
import os # Added for API key check
import asyncio
from datetime import datetime
from contextlib import asynccontextmanager

from google.adk.agents import LlmAgent
from google.adk.runners import Runner
from google.adk.sessions.in_memory_session_service import InMemorySessionService
from google.adk.artifacts import InMemoryArtifactService
from google.genai import types as genai_types # Renamed to avoid conflict

# Global Dapr client, to be initialized in lifespan
dapr_client: DaprClient = None # type: ignore

AGENT_ID = "worker-1" # Internal ID, can remain the same
AGENT_NAME = "LLM Worker" # New display name


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


# Define an ADK LlmAgent
class AdkLlmGreeterAgent(LlmAgent):
    def __init__(self, name: str = "AdkLlmGreeterAgent", model: str = "gemini-1.5-flash"):
        super().__init__(
            name=name,
            description="An ADK agent that greets or answers questions using an LLM.",
            instruction="You are a friendly and helpful agent. If given a name, greet the person warmly. If asked a question, provide a concise and accurate answer. If the question is complex, you can say you need more time or tools.",
            model=model
        )

# Global ADK Setup
# This agent instance can be named `root_agent` if ADK tooling expects that name.
root_agent = AdkLlmGreeterAgent()
session_service = InMemorySessionService()
artifact_service = InMemoryArtifactService()

ADK_APP_NAME = "worker_adk_application" # Used for ADK Runner and Session Service
adk_runner = Runner(
    app_name=ADK_APP_NAME,
    agent=root_agent,
    artifact_service=artifact_service,
    session_service=session_service
)

# Map incoming chat session IDs to ADK session IDs
adk_sessions_map: dict[str, str] = {}


async def _register_with_coordinator(): # Renamed and made internal
    """Register this worker with the coordinator"""
    print(f"Worker ({AGENT_ID}): Attempting to register with coordinator.")
    agent_data = {
        "id": AGENT_ID,
        "name": AGENT_NAME, # Will be "LLM Worker"
        "type": "llm",     # New type for UI display
        "status": "active"
    }
    
    # Use Dapr service invocation to register
    await dapr_client.invoke_method(
        app_id="coordinator",
        method_name="agents/register",
        data=json.dumps(agent_data),
        http_verb="POST"
    )
    print(f"Worker ({AGENT_ID}): Registration call completed.")

# Subscribe to job queue
# Note: DaprApp subscriptions are typically discovered at import time or when DaprApp is initialized.
# Ensure dapr_app is initialized after 'app = FastAPI(lifespan=lifespan)' if it depends on app instance.

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

# Heartbeat
async def send_heartbeat():
    """Send periodic heartbeat to coordinator"""
    while True:
        agent_data = {
            "id": AGENT_ID,
            "last_heartbeat": datetime.now().isoformat()
        }
        
        await dapr_client.save_state(
            store_name="statestore",
            key=f"agent-{AGENT_ID}",
            value=json.dumps(agent_data),
            state_metadata={"merge": "true"}
        )
        
        await asyncio.sleep(30)

@asynccontextmanager
async def lifespan(app: FastAPI):
    global dapr_client
    dapr_client = DaprClient()
    print("Worker DaprClient initialized in lifespan")

    await _register_with_coordinator()
    asyncio.create_task(send_heartbeat())
    print("Worker registration and heartbeat task started in lifespan")
    
    yield
    
    if dapr_client:
        print("Worker Closing DaprClient in lifespan")
        await dapr_client.close()
    dapr_client = None # type: ignore

app = FastAPI(lifespan=lifespan)
dapr_app = DaprApp(app) # Initialize DaprApp after app is created with lifespan

@dapr_app.subscribe(pubsub="pubsub", topic="job-queue")
async def process_job(event: CustomTopicEvent): # Use CustomTopicEvent
    """Process incoming jobs"""
    print(f"Worker Agent: Received job event. Raw event.data type: {type(event.data)}, content_type: {event.data_content_type}")
    job_data = event.data

    if isinstance(job_data, str):
        print(f"Worker Agent: event.data for job is a string. Attempting json.loads on: {repr(job_data)}")
        try:
            job_data = json.loads(job_data)
            print(f"Worker Agent: Successfully parsed string event.data for job. New type: {type(job_data)}")
        except json.JSONDecodeError as e:
            print(f"Worker Agent: Failed to decode JSON from string event.data for job: {e}. Original data: {repr(event.data)}")
            return {"status": "DROP", "error": "event.data string for job is not valid JSON"}
    elif not isinstance(job_data, dict):
        print(f"Worker Agent: event.data for job is neither a string nor a dict. Type: {type(job_data)}. Value: {repr(job_data)}")
        return {"status": "DROP", "error": "event.data for job has unexpected type"}

    try:
        print(f"Received job: {job_data['id']}")
    except KeyError:
        print(f"Worker Agent: 'id' key missing in job_data. Data: {repr(job_data)}")
        return {"status": "DROP", "error": "missing 'id' in job_data"}
    except TypeError:
        print(f"Worker Agent: job_data is not a dictionary, cannot access 'id'. Data: {repr(job_data)}")
        return {"status": "DROP", "error": "job_data not a dictionary"}
    
    # Update job status to processing
    job_data["status"] = "processing"
    job_data["agent_id"] = AGENT_ID
    
    await dapr_client.save_state(
        store_name="statestore",
        key=f"job-{job_data['id']}",
        value=json.dumps(job_data)
    )
    
    # Simulate job processing
    result = await execute_job(job_data)
    
    # Update job completion
    job_data["status"] = "completed"
    job_data["completed_at"] = datetime.now().isoformat()
    job_data["result"] = result
    
    # Publish completion event
    await dapr_client.publish_event(
        pubsub_name="pubsub",
        topic_name="job-completed",
        data=json.dumps(job_data), # Serialize to JSON string
        data_content_type="application/json" # Specify content type
    )
    
    return {"success": True}

@dapr_app.subscribe(pubsub="pubsub", topic="chat-messages")
async def handle_chat_message(event: CustomTopicEvent): # Use CustomTopicEvent
    """Handle incoming chat messages"""
    print(f"Worker Agent: Received chat event. Raw event.data type: {type(event.data)}, content_type: {event.data_content_type}")
    message_data = event.data

    if isinstance(message_data, str):
        print(f"Worker Agent: event.data for chat is a string. Attempting json.loads on: {repr(message_data)}")
        try:
            message_data = json.loads(message_data)
            print(f"Worker Agent: Successfully parsed string event.data for chat. New type: {type(message_data)}")
        except json.JSONDecodeError as e:
            print(f"Worker Agent: Failed to decode JSON from string event.data for chat: {e}. Original data: {repr(event.data)}")
            return {"status": "DROP", "error": "event.data string for chat is not valid JSON"}
    elif not isinstance(message_data, dict):
        print(f"Worker Agent: event.data for chat is neither a string nor a dict. Type: {type(message_data)}. Value: {repr(message_data)}")
        return {"status": "DROP", "error": "event.data for chat has unexpected type"}

    try:
        print(f"Chat message from {message_data['sender_id']}: {message_data['content']}")
    except KeyError as e:
        print(f"Worker Agent: Key {e} missing in chat message_data. Data: {repr(message_data)}")
        return {"status": "DROP", "error": f"missing key {e} in chat_data"}
    except TypeError:
        print(f"Worker Agent: chat message_data is not a dictionary. Data: {repr(message_data)}")
        return {"status": "DROP", "error": "chat_data not a dictionary"}
    
    # Process message and potentially respond
    original_content = message_data.get("content", "")
    response_payload = None

    if "@echo" in original_content:
        incoming_session_id = message_data.get("session_id")
        if not incoming_session_id:
            print(f"Worker ({AGENT_ID}): Received @echo request without session_id. Cannot reply specifically.")
            return {"status": "DROP", "error": "missing session_id in @echo request"}
        content_to_echo = original_content.replace("@echo", "").strip()
        response_payload = {
            "sender_id": AGENT_ID,
            "content": f"Echo: {content_to_echo}",
            "timestamp": datetime.now().isoformat(),
            "session_id": incoming_session_id
        }
        print(f"Worker ({AGENT_ID}): Sending echo reply: {response_payload}")

    elif "@llm" in original_content:
        # Prevent processing its own messages if they contain the command
        if message_data.get("sender_id") == AGENT_ID:
            print(f"Worker ({AGENT_ID}): Skipping self-generated message containing @llm: {original_content}")
            return {"status": "DROP", "error": "Skipping self-generated @llm message"}

        incoming_session_id = message_data.get("session_id")
        if not incoming_session_id:
            print(f"Worker ({AGENT_ID}): Received @llm request without session_id. Cannot reply specifically.")
            return {"status": "DROP", "error": "missing session_id in @llm request"}
        
        content_for_llm = original_content.replace("@llm", "").strip()
        llm_reply_text = ""
        current_adk_session_id = None # This is for ADK's internal session, distinct from chat session_id

        history_for_llm = []
        try:
            current_message_timestamp_str = message_data.get('timestamp')
            if not current_message_timestamp_str:
                print(f"Worker ({AGENT_ID}): Missing timestamp in current message. Cannot reliably fetch history.")
                all_messages = []
            else:
                current_message_timestamp = datetime.fromisoformat(current_message_timestamp_str)
                user_id_for_session = message_data.get('sender_id')

                print(f"Worker ({AGENT_ID}): Fetching chat history for session {incoming_session_id} for @llm command.")
                chat_history_response = await dapr_client.invoke_method(
                    app_id="chat",
                    method_name=f"chat/history/{incoming_session_id}",
                    data=None, # Explicitly pass data as None for GET
                    http_verb="GET"
                )
                if chat_history_response.data:
                    raw_history = json.loads(chat_history_response.data.decode())
                    all_messages = raw_history.get("messages", [])
                else:
                    all_messages = []
            
                for msg in all_messages:
                    msg_timestamp_str = msg.get('timestamp')
                    if not msg_timestamp_str:
                        continue 
                    
                    msg_timestamp = datetime.fromisoformat(msg_timestamp_str)
                    if msg_timestamp < current_message_timestamp:
                        role = None
                        text_content = msg.get('content', '')
                        if msg.get('sender_id') == AGENT_ID:
                            role = 'model'
                        elif msg.get('sender_id') == user_id_for_session:
                            role = 'user'
                            # Clean up @llm from historical user messages to avoid confusing the LLM
                            text_content = text_content.replace("@llm", "").strip()
                        
                        if role:
                            history_for_llm.append(genai_types.Content(role=role, parts=[genai_types.Part(text=text_content)]))
                print(f"Worker ({AGENT_ID}): Prepared {len(history_for_llm)} messages for LLM history.")

        except Exception as e:
            print(f"Worker ({AGENT_ID}): Error processing or fetching chat history for session {incoming_session_id}: {e}")
            # Proceed without history if an error occurs

        try:
            api_key = os.getenv("GOOGLE_API_KEY")
            if not api_key:
                print(f"Worker ({AGENT_ID}): GOOGLE_API_KEY not set. ADK LLM agent cannot be invoked.")
                llm_reply_text = "LLM Agent requires GOOGLE_API_KEY to be set."
            else:
                adk_user_id = "adk_fixed_user" 

                if incoming_session_id not in adk_sessions_map:
                    try:
                        new_adk_session_obj = await session_service.create_session(
                            app_name=ADK_APP_NAME,
                            user_id=adk_user_id
                        )
                        current_adk_session_id = new_adk_session_obj.id
                        adk_sessions_map[incoming_session_id] = current_adk_session_id
                        print(f"Worker ({AGENT_ID}): Created new ADK session {current_adk_session_id} for incoming chat session {incoming_session_id}")
                    except Exception as e:
                        print(f"Worker ({AGENT_ID}): Error creating ADK session: {e}")
                        llm_reply_text = f"Error initializing ADK session: {type(e).__name__}"
                else:
                    current_adk_session_id = adk_sessions_map[incoming_session_id]
                    print(f"Worker ({AGENT_ID}): Using existing ADK session {current_adk_session_id} for incoming chat session {incoming_session_id}")

                if current_adk_session_id and not llm_reply_text:
                    print(f"Worker ({AGENT_ID}): Invoking ADK Runner for ADK session {current_adk_session_id} with input: '{content_for_llm}' and {len(history_for_llm)} history messages.")
                    agent_reply_parts = []
                    
                    new_llm_message = genai_types.Content(role='user', parts=[genai_types.Part(text=content_for_llm)])

                    async for event in adk_runner.run_async(
                        user_id=adk_user_id,
                        session_id=current_adk_session_id,
                        history=history_for_llm if history_for_llm else None, # Changed to 'history'
                        new_message=new_llm_message
                    ):
                        if event.author != 'user' and event.content and event.content.parts:
                            for part in event.content.parts:
                                if part.text:
                                    agent_reply_parts.append(part.text)
                    
                    llm_reply_text = "".join(agent_reply_parts)
                    if not llm_reply_text:
                        llm_reply_text = "LLM agent did not return a text response."
                elif not llm_reply_text: 
                    llm_reply_text = "Failed to establish ADK session."

            response_payload = {
                "sender_id": AGENT_ID,
                "content": f"{llm_reply_text}",
                "timestamp": datetime.now().isoformat(),
                "session_id": incoming_session_id
            }
            print(f"Worker ({AGENT_ID}): Sending LLM reply: {response_payload}")
        except Exception as e:
            print(f"Worker ({AGENT_ID}): Error invoking LLM agent: {e}")
            response_payload = {
                "sender_id": AGENT_ID,
                "content": f"Error processing LLM request.", # Removed "@" to prevent loop
                "timestamp": datetime.now().isoformat(),
                "session_id": incoming_session_id
            }

    if response_payload:
        await dapr_client.publish_event(
            pubsub_name="pubsub",
            topic_name="chat-messages",
            data=json.dumps(response_payload), 
            data_content_type="application/json"
        )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)

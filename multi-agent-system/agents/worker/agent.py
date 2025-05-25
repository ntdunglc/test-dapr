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

from google.adk.agents import LlmAgent # Changed from Agent to LlmAgent
from google.adk.core import InferenceContext, InferenceRequest, InferenceResponse # StandardInput/Output not directly used by LlmAgent.invoke

# Global Dapr client, to be initialized in lifespan
dapr_client: DaprClient = None # type: ignore

AGENT_ID = "worker-1" # Internal ID, can remain the same
AGENT_NAME = "Echo Worker" # New display name


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


# Define an ADK LlmAgent based on the provided documentation
class AdkLlmGreeterAgent(LlmAgent):
    def __init__(self, name: str = "AdkLlmGreeterAgent", model: str = "gemini-1.5-flash-latest"): # Updated model name
        super().__init__(
            name=name,
            description="An ADK agent that greets or answers questions using an LLM.",
            instruction="You are a friendly and helpful agent. If given a name, greet the person warmly. If asked a question, provide a concise and accurate answer. If the question is complex, you can say you need more time or tools.",
            model=model
        )
        # The LlmAgent initializes its LLM client if an API key is available.

# Note: We are not creating a global root_agent instance here for AdkLlmGreeterAgent.
# It will be instantiated on demand within handle_chat_message to allow runtime API key check.


async def _register_with_coordinator(): # Renamed and made internal
    """Register this worker with the coordinator"""
    print(f"Worker ({AGENT_ID}): Attempting to register with coordinator.")
    agent_data = {
        "id": AGENT_ID,
        "name": AGENT_NAME, # Will be "Echo Worker"
        "type": "echo",    # New type for UI display
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
            "content": f"Echo: {content_to_echo} (session: {incoming_session_id[:6]})",
            "timestamp": datetime.now().isoformat(),
            "session_id": incoming_session_id
        }
        print(f"Worker ({AGENT_ID}): Sending echo reply: {response_payload}")

    elif "@adk" in original_content:
        incoming_session_id = message_data.get("session_id")
        if not incoming_session_id:
            print(f"Worker ({AGENT_ID}): Received @adk request without session_id. Cannot reply specifically.")
            return {"status": "DROP", "error": "missing session_id in @adk request"}
        
        content_for_adk = original_content.replace("@adk", "").strip()
        
        try:
            api_key = os.getenv("GOOGLE_API_KEY")
            if not api_key:
                print(f"Worker ({AGENT_ID}): GOOGLE_API_KEY not set. ADK LLM agent cannot be invoked.")
                adk_reply_text = "ADK LLM Agent requires GOOGLE_API_KEY to be set."
            else:
                # Create instance here to ensure it picks up the env var if set at runtime
                adk_llm_agent_instance = AdkLlmGreeterAgent()
                print(f"Worker ({AGENT_ID}): Invoking ADK LLM agent '{adk_llm_agent_instance.name}' with input: '{content_for_adk}'")

                # Invoke the ADK LlmAgent
                # The LlmAgent's invoke method is async
                adk_response_payload_dict = await adk_llm_agent_instance.invoke({"text": content_for_adk})
                
                if isinstance(adk_response_payload_dict, dict) and "text" in adk_response_payload_dict:
                    adk_reply_text = adk_response_payload_dict["text"]
                else:
                    print(f"Worker ({AGENT_ID}): Unexpected ADK response structure: {adk_response_payload_dict}")
                    adk_reply_text = f"ADK LLM processed, but response format was unexpected: {str(adk_response_payload_dict)[:100]}"
            
            response_payload = {
                "sender_id": AGENT_ID, 
                "content": f"{adk_reply_text} (session: {incoming_session_id[:6]})",
                "timestamp": datetime.now().isoformat(),
                "session_id": incoming_session_id
            }
            print(f"Worker ({AGENT_ID}): Sending ADK reply: {response_payload}")
        except Exception as e:
            print(f"Worker ({AGENT_ID}): Error invoking ADK agent: {e}")
            response_payload = {
                "sender_id": AGENT_ID,
                "content": f"Error processing @adk request. (session: {incoming_session_id[:6]})",
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

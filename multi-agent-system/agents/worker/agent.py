from fastapi import FastAPI
from dapr.aio.clients import DaprClient
from dapr.ext.fastapi import DaprApp
from pydantic import BaseModel, Field # Added Field
from typing import Any, Optional # Added Any, Optional
import json
import time
import os # Added for API key check
import asyncio
import traceback # Added for printing stack traces
import re # Added for regular expression matching
from datetime import datetime
from contextlib import asynccontextmanager

from dapr_agents import Agent as DaprAgent # For LLM functionality
from dapr_agents.memory import ConversationDaprStateMemory # For session-specific memory

# Global Dapr client, to be initialized in lifespan
dapr_client: DaprClient = None # type: ignore

AGENT_ID = "worker-1" # Internal ID, can remain the same
AGENT_NAME = "LLM Worker" # New display name
SUPPORTED_COMMANDS = ["@echo", "@llm"] # Commands this worker supports


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

# Store DaprAgent instances, keyed by chat session_id
# Each agent will have its own memory tied to that session.
dapr_agent_instances: dict[str, DaprAgent] = {}

async def _register_with_coordinator(): # Renamed and made internal
    """Register this worker with the coordinator"""
    print(f"Worker ({AGENT_ID}): Attempting to register with coordinator.")
    agent_data = {
        "id": AGENT_ID,
        "name": AGENT_NAME, # Will be "LLM Worker"
        "type": "llm",     # New type for UI display
        "status": "active",
        "supported_commands": SUPPORTED_COMMANDS
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
    
    elif task_type == "user_task":
        description = payload.get("description", "No description provided.")
        print(f"Worker ({AGENT_ID}): Processing user_task via LLM. Description: '{description}'")
        
        llm_response_text = "LLM processing failed or was not invoked." # Default
        job_session_id = job_data.get("id", "unknown_job_id") # Use job_id as session_id for DaprAgent memory

        try:
            openai_api_key = os.getenv("OPENAI_API_KEY")
            if not openai_api_key:
                print(f"Worker ({AGENT_ID}): OPENAI_API_KEY not set. LLM cannot be invoked for job {job_session_id}.")
                llm_response_text = "LLM Agent requires OPENAI_API_KEY to be set."
            else:
                if job_session_id not in dapr_agent_instances:
                    print(f"Worker ({AGENT_ID}): Creating new DaprAgent (OpenAI backend) for job-session {job_session_id}")
                    session_memory = ConversationDaprStateMemory(
                        store_name="statestore", 
                        session_id=job_session_id, # Tie memory to job_id
                        key_prefix="conversation-", # Align with chat agent's storage key
                        dapr_client=dapr_client
                    )
                    agent_instance = DaprAgent(
                        name=f"OpenAIAgentJob-{job_session_id[:6]}",
                        role="Job Processing AI Assistant (OpenAI)",
                        goal="Process the given task description and provide a comprehensive result.",
                        instructions=[
                            "You are an AI assistant processing a job task.",
                            "Provide a detailed and accurate response to the task description.",
                            "The user is not directly conversing, this is an automated job execution."
                        ],
                        memory=session_memory,
                        tools=[], 
                        model="gpt-3.5-turbo",
                    )
                    dapr_agent_instances[job_session_id] = agent_instance
                else:
                    print(f"Worker ({AGENT_ID}): Using existing DaprAgent (OpenAI backend) for job-session {job_session_id}")
                
                current_dapr_agent = dapr_agent_instances[job_session_id]
                print(f"Worker ({AGENT_ID}): Invoking DaprAgent for job {job_session_id} with input: '{description}'.")
                
                agent_response = await current_dapr_agent.run(description)
                
                if isinstance(agent_response, str):
                    llm_response_text = agent_response
                elif hasattr(agent_response, 'content') and isinstance(agent_response.content, str):
                    llm_response_text = agent_response.content
                else:
                    llm_response_text = str(agent_response)

                if not llm_response_text.strip():
                    llm_response_text = "LLM agent did not return a text response for the job."

        except Exception as e:
            print(f"Worker ({AGENT_ID}): Error invoking Dapr LLM agent for job {job_session_id}: {e}")
            traceback.print_exc()
            llm_response_text = f"Error processing job task with LLM: {str(e)}"

        return {
            "llm_response": llm_response_text, # This will be the main output
            "status_notes": f"Processed by {AGENT_NAME} ({AGENT_ID})",
            "original_task_description": description
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
        job_id = job_data['id']
        target_agent_id = job_data.get("agent_id")
        print(f"Worker ({AGENT_ID}): Received job: {job_id}. Target agent: {target_agent_id}")
    except KeyError:
        print(f"Worker Agent ({AGENT_ID}): 'id' key missing in job_data. Data: {repr(job_data)}")
        return {"status": "DROP", "error": "missing 'id' in job_data"}
    except TypeError:
        print(f"Worker Agent ({AGENT_ID}): job_data is not a dictionary, cannot access 'id'. Data: {repr(job_data)}")
        return {"status": "DROP", "error": "job_data not a dictionary"}

    # Check if the job is assigned to a specific agent and if it's this agent
    if target_agent_id and target_agent_id != AGENT_ID:
        print(f"Worker ({AGENT_ID}): Job {job_id} is for agent {target_agent_id}, not me. Skipping.")
        # This worker will not process it. Another worker with the correct ID should.
        # If Dapr's pub/sub is used with competing consumers, this message might be effectively lost
        # if no other consumer picks it up or if redelivery isn't configured robustly for this scenario.
        # For true targeted delivery, direct invocation or agent-specific topics would be better.
        # For now, we just acknowledge and don't process.
        return {"status": "RETRY"} # Or "SUCCESS" if we don't want Dapr to retry with this consumer.
                                    # "DROP" if the message is malformed/unrecoverable.
                                    # Let's use "SUCCESS" to indicate we've seen it but it's not for us.
                                    # Dapr default is to ACK on 2xx.
                                    # If we want other workers to get a chance, this worker should not "complete" it.
                                    # However, with pub/sub, all subscribers get a copy.
                                    # The current model is that any worker *can* pick any job.
                                    # The filtering here is an application-level decision.
                                    # So, if it's not for me, I just successfully do nothing.
        return {"status": "SUCCESS", "message": f"Job {job_id} not for this agent."}


    # Update job status to processing
    job_data["status"] = "processing"
    job_data["agent_id"] = AGENT_ID
    
    await dapr_client.save_state(
        store_name="statestore",
        key=f"job-{job_data['id']}",
        value=json.dumps(job_data)
    )
    
    # Simulate job processing
    result = await execute_job(job_data) # result is now a dict, potentially with "llm_response"
    
    # If the job was a user_task and an LLM response was generated, send it as a chat message
    if job_data.get("task_type") == "user_task" and isinstance(result, dict) and "llm_response" in result:
        llm_chat_content = result["llm_response"]
        
        job_session_id_for_agent = job_data['id'] # This was used as session_id for the agent
        agent_for_job = dapr_agent_instances.get(job_session_id_for_agent)
        
        ai_sender_id_for_job_chat = AGENT_ID # Default
        if agent_for_job and hasattr(agent_for_job, 'name'):
            ai_sender_id_for_job_chat = agent_for_job.name
        else:
            print(f"Worker ({AGENT_ID}): Warning - DaprAgent instance for job {job_session_id_for_agent} (or its name) not found in cache when preparing chat message. Defaulting sender_id to {AGENT_ID}.")

        chat_payload = {
            "sender_id": ai_sender_id_for_job_chat, # Use the specific agent's name
            "content": llm_chat_content,
            "timestamp": datetime.now().isoformat(),
            "session_id": job_data['id'] # Use job_id as session_id for the chat message
        }
        print(f"Worker ({AGENT_ID}): Sending LLM job response as chat message from sender '{ai_sender_id_for_job_chat}' for job {job_data['id']}")
        await dapr_client.publish_event(
            pubsub_name="pubsub",
            topic_name="chat-messages",
            data=json.dumps(chat_payload), 
            data_content_type="application/json"
        )

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
    # The incoming_session_id could be a regular session_id or a job_id if the chat is job-focused
    incoming_session_id = message_data.get("session_id") 
    response_payload = None
    sender_is_self = message_data.get("sender_id") == AGENT_ID

    if not incoming_session_id:
        print(f"Worker ({AGENT_ID}): Received chat message without session_id. Cannot process or reply specifically. Data: {message_data}")
        return {"status": "DROP", "error": "missing session_id in chat message"}

    # Priority 1: @echo command
    if "@echo" in original_content:
        if not sender_is_self: # Don't echo own echos
            content_to_echo = original_content.replace("@echo", "").strip()
            response_payload = {
                "sender_id": AGENT_ID,
                "content": f"Echo from {AGENT_NAME}: {content_to_echo}",
                "timestamp": datetime.now().isoformat(),
                "session_id": incoming_session_id
            }
            print(f"Worker ({AGENT_ID}): Sending echo reply to session/job {incoming_session_id}: {response_payload['content']}")
        else:
            print(f"Worker ({AGENT_ID}): Skipping self-sent @echo command.")
            return {"status": "SUCCESS"} # Successfully did nothing

    # Determine if it's an explicit LLM call and prepare content for LLM
    is_explicit_llm_call = False
    # Default to original stripped content if not an explicit call but active_agent will handle it
    content_for_llm_input = original_content.strip() 

    # Check for "@llm command" (case-insensitive, requires space after @llm)
    # re.DOTALL allows . to match newline characters if the content spans multiple lines
    explicit_llm_match = re.match(r"@llm\s+(.*)", original_content.strip(), re.IGNORECASE | re.DOTALL)
    if explicit_llm_match:
        is_explicit_llm_call = True
        content_for_llm_input = explicit_llm_match.group(1).strip()
    elif original_content.strip().lower() == "@llm": # Handle case where only "@llm" is typed
        is_explicit_llm_call = True
        content_for_llm_input = "" # Explicit call with no content for LLM

    # Condition to invoke LLM
    is_active_agent = message_data.get("active_agent_id") == AGENT_ID
    should_invoke_llm = (response_payload is None) and \
                        (not sender_is_self) and \
                        (is_explicit_llm_call or is_active_agent)

    if should_invoke_llm:
        # If it's an implicit call (is_active_agent is true, is_explicit_llm_call is false),
        # content_for_llm_input is already original_content.strip().
        # If it's an explicit call, content_for_llm_input has been adjusted.

        # If the LLM is being invoked and the resulting content for it is empty, then do nothing.
        if not content_for_llm_input:
            print(f"Worker ({AGENT_ID}): No content for LLM. Explicit call: {is_explicit_llm_call}, Active agent: {is_active_agent}. Session: {incoming_session_id}")
            return {"status": "SUCCESS"} 

        llm_reply_text = ""
        # Use content_for_llm_input for the LLM
        print(f"Worker ({AGENT_ID}): Preparing to invoke LLM. Active: {is_active_agent}, Explicit: {is_explicit_llm_call}. Content: '{content_for_llm_input}'")

        try:
            openai_api_key = os.getenv("OPENAI_API_KEY")
            if not openai_api_key:
                print(f"Worker ({AGENT_ID}): OPENAI_API_KEY not set. OpenAI LLM agent cannot be invoked.")
                llm_reply_text = "LLM Agent requires OPENAI_API_KEY to be set."
            else:
                if incoming_session_id not in dapr_agent_instances:
                    print(f"Worker ({AGENT_ID}): Creating new DaprAgent (OpenAI backend) for session {incoming_session_id}")
                    session_memory = ConversationDaprStateMemory(
                        store_name="statestore",
                        session_id=f"conversation-{incoming_session_id}",
                        dapr_client=dapr_client,
                    )
                    agent_instance = DaprAgent(
                        name=f"OpenAIAgentSession-{incoming_session_id[:6]}",
                        role="Conversational AI Assistant (OpenAI)",
                        goal="Assist users with their queries accurately and concisely using OpenAI.",
                        instructions=[
                            "You are a helpful AI assistant powered by OpenAI.",
                            "Provide clear and concise answers.",
                            "If you don't know the answer, say so."
                        ],
                        memory=session_memory,
                        tools=[], 
                        model="gpt-3.5-turbo",
                    )
                    dapr_agent_instances[incoming_session_id] = agent_instance
                else:
                    print(f"Worker ({AGENT_ID}): Using existing DaprAgent (OpenAI backend) for session {incoming_session_id}")

                current_dapr_agent = dapr_agent_instances[incoming_session_id]
                print(f"Worker ({AGENT_ID}): Invoking DaprAgent (OpenAI backend) for session {incoming_session_id} with input: '{content_for_llm_input}'.")

                agent_response = await current_dapr_agent.run(content_for_llm_input)

                if isinstance(agent_response, str):
                    llm_reply_text = agent_response
                elif hasattr(agent_response, 'content') and isinstance(agent_response.content, str):
                    llm_reply_text = agent_response.content
                else:
                    llm_reply_text = str(agent_response)

                if not llm_reply_text.strip():
                    llm_reply_text = "LLM agent did not return a text response."

            response_payload = {
                "sender_id": current_dapr_agent.name, # Use the agent's actual name
                "content": llm_reply_text, 
                "timestamp": datetime.now().isoformat(),
                "session_id": incoming_session_id
            }
            print(f"Worker ({AGENT_ID}): Sending LLM reply via DaprAgent (OpenAI backend) as sender '{current_dapr_agent.name}': {response_payload}")

        except Exception as e:
            print(f"Worker ({AGENT_ID}): Error invoking Dapr LLM agent (OpenAI backend) for agent '{current_dapr_agent.name}': {e}")
            traceback.print_exc()
            response_payload = {
                "sender_id": AGENT_ID,
                "content": "Error processing LLM request.",
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

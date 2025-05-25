import asyncio
import json
import warnings # Add import for warnings module
from dapr.aio.clients import DaprClient

STATE_STORE_NAME = "statestore"
ALL_SESSIONS_LIST_KEY = "_internal_all_session_ids" # Must match the key in coordinator
ALL_JOBS_LIST_KEY = "_internal_all_job_ids"         # Must match the key in coordinator

async def clear_all_data():
    """
    Clears all chat session-related and job-related data from the Dapr state store.
    - Chat Sessions:
        - Fetches the list of all chat session IDs from ALL_SESSIONS_LIST_KEY.
        - Deletes each 'coordinator-session-<id>'.
        - Deletes each 'conversation-<id>' (chat history for the session).
        - Deletes ALL_SESSIONS_LIST_KEY itself.
    - Jobs:
        - Fetches the list of all job IDs from ALL_JOBS_LIST_KEY.
        - Deletes each job object (key is the job_id).
        - Deletes each 'conversation-<job_id>' (chat history for the job).
        - Deletes ALL_JOBS_LIST_KEY itself.
    """
    # Suppress the specific RuntimeWarning from Dapr SDK about unawaited Channel.close.
    # This is a known issue/behavior in some versions of the Dapr Python SDK's gRPC client.
    # The script's core functionality is not affected; this just cleans up log output.
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message="coroutine 'Channel.close' was never awaited",
            category=RuntimeWarning,
            module="dapr.aio.clients.grpc.client"
        )
        async with DaprClient() as d:
            print(f"Attempting to clear all session and job data from state store: {STATE_STORE_NAME}")

            # --- Clear Chat Session Data ---
            print("\n--- Processing Chat Sessions ---")
            all_session_ids = []
            try:
                state = await d.get_state(store_name=STATE_STORE_NAME, key=ALL_SESSIONS_LIST_KEY)
                if state.data:
                    all_session_ids = json.loads(state.data)
                    print(f"Found {len(all_session_ids)} chat session IDs in list '{ALL_SESSIONS_LIST_KEY}': {all_session_ids}")
                else:
                    print(f"Chat session ID list '{ALL_SESSIONS_LIST_KEY}' not found or empty.")
            except Exception as e:
                print(f"Error fetching chat session ID list '{ALL_SESSIONS_LIST_KEY}': {e}")

            if all_session_ids:
                for session_id in all_session_ids:
                    coordinator_key = f"coordinator-session-{session_id}"
                    conversation_key = f"conversation-{session_id}"
                    
                    try:
                        await d.delete_state(store_name=STATE_STORE_NAME, key=coordinator_key)
                        print(f"  Deleted coordinator session key: {coordinator_key}")
                    except Exception as e:
                        print(f"  Error deleting key {coordinator_key}: {e} (may not exist)")

                    try:
                        await d.delete_state(store_name=STATE_STORE_NAME, key=conversation_key)
                        print(f"  Deleted conversation history key: {conversation_key}")
                    except Exception as e:
                        print(f"  Error deleting key {conversation_key}: {e} (may not exist)")
            
            try:
                await d.delete_state(store_name=STATE_STORE_NAME, key=ALL_SESSIONS_LIST_KEY)
                print(f"Deleted chat session ID list key: {ALL_SESSIONS_LIST_KEY}")
            except Exception as e:
                print(f"Error deleting key {ALL_SESSIONS_LIST_KEY}: {e} (may not exist)")

            # --- Clear Job Data ---
            print("\n--- Processing Jobs ---")
            all_job_ids = []
            try:
                state = await d.get_state(store_name=STATE_STORE_NAME, key=ALL_JOBS_LIST_KEY)
                if state.data:
                    # Assuming job IDs are stored as a JSON list, similar to session IDs
                    all_job_ids = json.loads(state.data) 
                    print(f"Found {len(all_job_ids)} job IDs in list '{ALL_JOBS_LIST_KEY}': {all_job_ids}")
                else:
                    print(f"Job ID list '{ALL_JOBS_LIST_KEY}' not found or empty.")
            except Exception as e:
                print(f"Error fetching job ID list '{ALL_JOBS_LIST_KEY}': {e}")

            if all_job_ids:
                for job_id in all_job_ids:
                    # Job object itself is stored with job_id as key by the coordinator
                    job_object_key = job_id 
                    job_conversation_key = f"conversation-{job_id}" # Chat history for the job
                    
                    try:
                        await d.delete_state(store_name=STATE_STORE_NAME, key=job_object_key)
                        print(f"  Deleted job object key: {job_object_key}")
                    except Exception as e:
                        print(f"  Error deleting key {job_object_key}: {e} (may not exist)")

                    try:
                        await d.delete_state(store_name=STATE_STORE_NAME, key=job_conversation_key)
                        print(f"  Deleted conversation history key for job: {job_conversation_key}")
                    except Exception as e:
                        print(f"  Error deleting key {job_conversation_key}: {e} (may not exist)")
            
            try:
                await d.delete_state(store_name=STATE_STORE_NAME, key=ALL_JOBS_LIST_KEY)
                print(f"Deleted job ID list key: {ALL_JOBS_LIST_KEY}")
            except Exception as e:
                print(f"Error deleting key {ALL_JOBS_LIST_KEY}: {e} (may not exist)")

            print("\nData clearing process complete.")

if __name__ == "__main__":
    # Ensure DAPR_GRPC_PORT and DAPR_HTTP_PORT are set if running standalone,
    # or run with 'dapr exec -- python scripts/clear_sessions.py'
    print("Starting data clearing script (sessions and jobs)...")
    asyncio.run(clear_all_data())
    print("Data clearing script finished.")

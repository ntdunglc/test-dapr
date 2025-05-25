import asyncio
import json
import warnings # Add import for warnings module
from dapr.aio.clients import DaprClient

STATE_STORE_NAME = "statestore"
ALL_SESSIONS_LIST_KEY = "_internal_all_session_ids" # Must match the key in coordinator

async def clear_all_sessions():
    """
    Clears all session-related data from the Dapr state store.
    - Fetches the list of all session IDs.
    - Deletes each 'coordinator-session-<id>'.
    - Deletes each 'conversation-<id>' (chat history).
    - Deletes the list of all session IDs itself.
    Note: This does not delete individual 'chat-<timestamp>' messages if they exist
    outside the conversation history lists, as that would require scanning all keys.
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
            print(f"Attempting to clear all session data from state store: {STATE_STORE_NAME}")

            # 1. Fetch the list of all session IDs
            all_session_ids = []
            try:
                state = await d.get_state(store_name=STATE_STORE_NAME, key=ALL_SESSIONS_LIST_KEY)
                if state.data:
                    all_session_ids = json.loads(state.data)
                    print(f"Found {len(all_session_ids)} session IDs in the global list: {all_session_ids}")
                else:
                    print(f"Global session ID list '{ALL_SESSIONS_LIST_KEY}' not found or empty.")
            except Exception as e:
                print(f"Error fetching global session ID list '{ALL_SESSIONS_LIST_KEY}': {e}")
                # Proceeding to attempt deletion of the key itself, in case it's corrupted.

            # 2. Delete individual session data
            if all_session_ids:
                for session_id in all_session_ids:
                    coordinator_key = f"coordinator-session-{session_id}"
                    conversation_key = f"conversation-{session_id}"
                    
                    try:
                        await d.delete_state(store_name=STATE_STORE_NAME, key=coordinator_key)
                        print(f"Deleted coordinator session key: {coordinator_key}")
                    except Exception as e:
                        print(f"Error deleting key {coordinator_key}: {e} (may not exist)")

                    try:
                        await d.delete_state(store_name=STATE_STORE_NAME, key=conversation_key)
                        print(f"Deleted conversation history key: {conversation_key}")
                    except Exception as e:
                        print(f"Error deleting key {conversation_key}: {e} (may not exist)")
            
            # 3. Delete the global list of session IDs
            try:
                await d.delete_state(store_name=STATE_STORE_NAME, key=ALL_SESSIONS_LIST_KEY)
                print(f"Deleted global session ID list key: {ALL_SESSIONS_LIST_KEY}")
            except Exception as e:
                print(f"Error deleting key {ALL_SESSIONS_LIST_KEY}: {e} (may not exist)")

            print("Session clearing process complete.")

if __name__ == "__main__":
    # Ensure DAPR_GRPC_PORT and DAPR_HTTP_PORT are set if running standalone,
    # or run with 'dapr exec -- python scripts/clear_sessions.py'
    print("Starting session clearing script...")
    asyncio.run(clear_all_sessions())
    print("Session clearing script finished.")

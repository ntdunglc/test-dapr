import asyncio
import argparse
import json
from dapr.aio.clients import DaprClient

STATE_STORE_NAME = "statestore"  # Should match the one used in chat/app.py

async def get_conversation_state(session_id: str):
    """
    Fetches and prints the conversation state for a given session_id from Dapr.
    """
    dapr_client = None
    try:
        dapr_client = DaprClient()
        print(f"Attempting to fetch state for session_id: {session_id}")
        
        conversation_key = f"conversation-{session_id}"
        
        state = await dapr_client.get_state(
            store_name=STATE_STORE_NAME,
            key=conversation_key
        )
        
        if state.data:
            print(f"\n--- Conversation State for session_id: {session_id} (key: {conversation_key}) ---")
            try:
                # Assuming the data is stored as a JSON string representing a list
                messages_list = json.loads(state.data)
                print(json.dumps(messages_list, indent=2))
            except json.JSONDecodeError:
                print("Error: Could not decode state data as JSON. Raw data:")
                print(state.data)
            except Exception as e:
                print(f"Error processing state data: {e}")
                print("Raw data:")
                print(state.data)
        else:
            print(f"No conversation state found for session_id: {session_id} (key: {conversation_key})")
            
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if dapr_client:
            print("\nClosing Dapr client.")
            await dapr_client.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch conversation state from Dapr.")
    parser.add_argument("session_id", type=str, help="The session ID to fetch the conversation state for.")
    
    args = parser.parse_args()
    
    asyncio.run(get_conversation_state(args.session_id))

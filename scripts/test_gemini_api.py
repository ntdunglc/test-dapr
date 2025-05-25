import os
import asyncio
from openai import OpenAI, APIError

async def test_gemini_api():
    """
    Tests the GEMINI_API_KEY by making a simple call to the Gemini API
    via its OpenAI-compatible endpoint.
    """
    api_key = os.getenv("GEMINI_API_KEY")

    if not api_key:
        print("Error: GEMINI_API_KEY environment variable not set.")
        print("Please set the GEMINI_API_KEY environment variable and try again.")
        return

    print(f"Attempting to use GEMINI_API_KEY: ...{api_key[-4:] if len(api_key) > 4 else '****'}")

    client = OpenAI(
        api_key=api_key,
        base_url="https://generativelanguage.googleapis.com/v1beta/openai/"
    )

    model_to_test = "gemini-2.0-flash" # Using the model from previous successful context
    # You can also try "gemini-1.5-flash-latest" or "gemini-pro"

    print(f"Sending request to Gemini model: {model_to_test}...")

    try:
        response = await asyncio.to_thread(
            client.chat.completions.create,
            model=model_to_test,
            messages=[
                {"role": "system", "content": "You are a helpful assistant."},
                {"role": "user", "content": "Hello! This is a test call. Respond with a short greeting."}
            ],
            max_tokens=50
        )
        print("\nAPI Call Successful!")
        print("Response:")
        if response.choices:
            print(f"  Choice 1 Message: {response.choices[0].message.content}")
        else:
            print("  No choices returned in the response.")
        
        if hasattr(response, 'usage') and response.usage:
            print(f"  Usage: {response.usage}")

    except APIError as e:
        print("\nAPI Call Failed!")
        print(f"  Error Type: {type(e).__name__}")
        print(f"  Status Code: {e.status_code}")
        print(f"  Message: {e.message}")
        if e.body:
            print(f"  Error Body: {e.body}")
        print("\nPlease double-check your GEMINI_API_KEY, ensure the Generative Language API is enabled in your Google Cloud project, and verify your billing/quota status.")
    except Exception as e:
        print("\nAn unexpected error occurred:")
        print(f"  Error Type: {type(e).__name__}")
        print(f"  Message: {str(e)}")

if __name__ == "__main__":
    print("Starting Gemini API Key Test Script...")
    # For .env file loading, you could add:
    # from dotenv import load_dotenv
    # load_dotenv()
    asyncio.run(test_gemini_api())
    print("\nTest script finished.")

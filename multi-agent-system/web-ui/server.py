from fastapi import FastAPI, Request, Response, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
import httpx
import websockets # Added for WebSocket client functionality
import asyncio # Added for asyncio.gather

app = FastAPI()

COORDINATOR_HOST = "localhost" # Assuming coordinator runs on localhost
COORDINATOR_PORT = 8000 # Coordinator's port

# Mount static files
app.mount("/static", StaticFiles(directory="static"), name="static")


@app.websocket("/ws/{client_id}")
async def websocket_proxy_endpoint(websocket: WebSocket, client_id: str):
    await websocket.accept()
    coordinator_ws_url = f"ws://{COORDINATOR_HOST}:{COORDINATOR_PORT}/ws/{client_id}"
    
    try:
        async with websockets.connect(coordinator_ws_url) as coordinator_socket:
            print(f"Proxy: Connected to coordinator WebSocket for client {client_id}")

            async def relay_to_coordinator():
                """Relay messages from client to coordinator."""
                try:
                    while True:
                        data = await websocket.receive_text()
                        await coordinator_socket.send(data)
                except WebSocketDisconnect:
                    print(f"Proxy: Client {client_id} disconnected (relay_to_coordinator).")
                except websockets.exceptions.ConnectionClosed:
                    print(f"Proxy: Coordinator connection closed while relaying from client {client_id}.")
                except Exception as e:
                    print(f"Proxy: Error relaying to coordinator for {client_id}: {e}")


            async def relay_to_client():
                """Relay messages from coordinator to client."""
                try:
                    while True:
                        data = await coordinator_socket.recv()
                        await websocket.send_text(data)
                except WebSocketDisconnect: # Should not happen on coordinator_socket.recv()
                    print(f"Proxy: Client disconnected unexpectedly (relay_to_client).")
                except websockets.exceptions.ConnectionClosed:
                    print(f"Proxy: Coordinator connection closed for client {client_id} (relay_to_client).")
                except Exception as e:
                    print(f"Proxy: Error relaying to client for {client_id}: {e}")

            # Run both relay tasks concurrently
            # If one task finishes (e.g., due to disconnection), the other will be cancelled.
            await asyncio.gather(
                relay_to_coordinator(),
                relay_to_client()
            )
    except websockets.exceptions.InvalidURI:
        print(f"Proxy: Invalid WebSocket URI for coordinator: {coordinator_ws_url}")
        await websocket.close(code=1011, reason="Proxy configuration error")
    except websockets.exceptions.ConnectionClosedError as e:
        print(f"Proxy: Could not connect to coordinator WebSocket at {coordinator_ws_url}: {e}")
        await websocket.close(code=1011, reason="Proxy target unavailable") # 1011: Server error
    except WebSocketDisconnect:
        print(f"Proxy: Client {client_id} disconnected before coordinator connection was fully handled.")
    except Exception as e:
        print(f"Proxy: Unhandled WebSocket proxy error for client {client_id}: {e}")
        # Ensure client websocket is closed if an error occurs before or during `gather`
        if websocket.client_state != WebSocketState.DISCONNECTED: # type: ignore
             await websocket.close(code=1011) # type: ignore
    finally:
        print(f"Proxy: WebSocket connection for client {client_id} ended.")


@app.get("/")
async def read_index():
    return FileResponse('static/index.html')

# Proxy API calls (HTTP)
@app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "DELETE"])
async def http_proxy(path: str, request: Request): # Renamed to http_proxy for clarity
    # Ensure WebSocket requests are not handled by this proxy
    if path.startswith("ws/"): # Or check request.scope['type'] == 'websocket' if needed earlier
        # This should ideally not be hit if the @app.websocket route is correctly placed and matched.
        # However, as a safeguard:
        print(f"HTTP Proxy: Received WebSocket path '{path}', but should be handled by WebSocket proxy. Ignoring.")
        return Response(status_code=426, content="Upgrade Required (Use WebSocket protocol)")


    async with httpx.AsyncClient() as client:
        url: str
        # Route chat history requests to the chat agent, others to coordinator
        if path.startswith("api/chat/history/"):
            # Extract the part after "api/" to match the chat agent's endpoint structure
            # e.g., if path is "api/chat/history/some-id", actual_chat_path becomes "chat/history/some-id"
            actual_chat_path = path[len("api/"):] 
            url = f"http://localhost:8004/{actual_chat_path}"
        else:
            # Forward other requests to coordinator's application port
            url = f"http://{COORDINATOR_HOST}:{COORDINATOR_PORT}/{path}"
            
        response = await client.request(
            method=request.method,
            url=url,
            content=await request.body(),
            headers=dict(request.headers)
        )
        
        # httpx's response.content provides the decoded response body as bytes.
        response_body_bytes = response.content

        # Prepare headers for the FastAPI response
        # Filter out headers that should not be blindly proxied,
        # especially those related to encoding or connection management.
        excluded_headers = {"transfer-encoding", "connection", "content-encoding", "content-length"}
        proxied_headers = {
            key: value for key, value in response.headers.items()
            if key.lower() not in excluded_headers
        }
        # Ensure 'content-type' is preserved if it exists and not already in proxied_headers (case-insensitively)
        # (Note: proxied_headers keys are already lowercased by the comprehension's key.lower() check logic,
        # so direct check for 'content-type' is fine)
        if 'content-type' not in {k.lower() for k in proxied_headers.keys()} and response.headers.get('content-type'):
            proxied_headers['content-type'] = response.headers['content-type']

        return Response(
            content=response_body_bytes,
            status_code=response.status_code,
            headers=proxied_headers
        )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=3000, debug=True)

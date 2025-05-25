from fastapi import FastAPI, Request, Response
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
import httpx

app = FastAPI()

# Mount static files
app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/")
async def read_index():
    return FileResponse('static/index.html')

# Proxy API calls to coordinator
@app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "DELETE"])
async def proxy(path: str, request: Request):
    async with httpx.AsyncClient() as client:
        url: str
        # Route chat history requests to the chat agent, others to coordinator
        if path == "api/chat/history/global":
            # Forward to chat agent's application port and specific endpoint
            url = "http://localhost:8002/chat/history/global"
        else:
            # Forward other requests to coordinator's application port
            url = f"http://localhost:8000/{path}"
            
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
    uvicorn.run(app, host="0.0.0.0", port=8080, debug=True)

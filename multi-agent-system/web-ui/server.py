from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
import httpx

app = FastAPI()

# Mount static files
app.mount("/static", StaticFiles(directory="static"), name="static")

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
        
        return response.content

@app.get("/")
async def read_index():
    return FileResponse('static/index.html')

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080, debug=True)

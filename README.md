# Multi-Agent System with Dapr and Python

This project demonstrates a multi-agent system built using Dapr and Python. It includes features like state persistence, session management, real-time chat, background job processing, and a web UI for monitoring.

## Prerequisites

1.  **Install Dapr CLI:**
    Follow the official Dapr installation guide: [https://docs.dapr.io/getting-started/install-dapr-cli/](https://docs.dapr.io/getting-started/install-dapr-cli/)

2.  **Initialize Dapr:**
    If you haven't already, initialize Dapr in your local environment (this will typically set up Redis for state store and pub/sub).
    ```bash
    dapr init
    ```

3.  **Python 3.8+:**
    Ensure you have Python 3.8 or a newer version installed.

## Project Structure

```
multi-agent-system/
├── components/             # Dapr component configurations (statestore, pubsub, etc.)
│   ├── statestore.yaml
│   ├── pubsub.yaml
│   └── bindings.yaml
│   └── dapr-tracing-config.yaml
├── agents/                 # Python applications for different agents
│   ├── coordinator/
│   │   ├── app.py
│   │   └── requirements.txt
│   ├── worker/  # Contains the Echo Worker agent
│   │   ├── app.py
│   │   └── requirements.txt
│   └── chat/
│       ├── app.py
│       └── requirements.txt
├── web-ui/                 # FastAPI server and static files for the web dashboard
│   ├── static/
│   │   ├── index.html
│   │   ├── style.css
│   │   └── app.js
│   ├── server.py
│   └── requirements.txt
├── dapr.yaml               # Dapr multi-app run configuration
├── README.md               # This file
└── dapr_test/
    └── claude_plan.md      # Original planning document
```

## Running the System

Follow these steps from the root directory of the `multi-agent-system` project.

1.  **Install Dependencies:**
    Install the necessary Python packages for each agent and the web UI.
    ```bash
    pip install -r agents/coordinator/requirements.txt
    pip install -r agents/worker/requirements.txt
    pip install -r agents/chat/requirements.txt
    pip install -r web-ui/requirements.txt
    ```
    *Note: If you encounter issues with `pydantic-core` during installation, especially on newer Python versions or specific OS/architectures, you might need to ensure you have Rust/Cargo installed or try an older compatible version of `pydantic` or related libraries.*

2.  **Start Dapr Applications:**
    Use the Dapr CLI to run all defined applications (coordinator, echo_worker, chat) as specified in `dapr.yaml`. Execute this command from the `multi-agent-system` root directory:
    ```bash
    dapr run -f .
    ```
    This command will start the coordinator, echo_worker, and chat agents, each with its own Dapr sidecar. You will see logs from Dapr and the individual applications in your terminal.

3.  **Start the Web UI:**
    In a **new terminal window**, navigate to the `web-ui` directory and start the FastAPI server for the UI.
    ```bash
    cd web-ui
    python server.py
    ```
    This server proxies API requests to the coordinator and serves the static HTML, CSS, and JavaScript files.

4.  **Access the Dashboard:**
    Open your web browser and navigate to:
    [http://localhost:8080](http://localhost:8080)

    You should see the Multi-Agent System Dashboard, where you can view agent statuses, submit jobs, and use the chat interface.

## Troubleshooting

*   **Redis Connection Issues:**
    *   Ensure Redis is running. `dapr init` usually installs and runs it in a Docker container. You can check with `docker ps`.
    *   Verify the Redis host and port in `components/statestore.yaml` and `components/pubsub.yaml` (default is `localhost:6379`).
*   **Authentication Issues (e.g., `/protected` route returns "Not authenticated" or token errors):**
    *   When testing authentication endpoints like `/auth/login` and `/protected` with `curl`, ensure you are correctly extracting the `access_token` from the JSON response. Using a tool like `jq` is more reliable than `sed` for parsing JSON. For example:
        ```bash
        TOKEN_RESPONSE=$(curl -s -X POST -d "username=testuser&password=testpass" http://localhost:8000/auth/login)
        ACCESS_TOKEN=$(echo $TOKEN_RESPONSE | jq -r .access_token)
        echo "Access Token: $ACCESS_TOKEN"
        curl -v -X GET -H "Authorization: Bearer $ACCESS_TOKEN" http://localhost:8000/protected
        ```
    *   Check the coordinator logs for specific `JWTError` messages if you receive "Could not validate credentials". The error type (e.g., `InvalidSignatureError`, `ExpiredSignatureError`) will be logged and included in the HTTP response.
    *   Ensure the `SECRET_KEY` in `agents/coordinator/app.py` is consistent and used correctly (encoded to bytes) for both encoding and decoding tokens.
*   **WebSocket Connection Fails:**
    *   Ensure the coordinator agent (`appID: coordinator`) is running correctly (check Dapr logs). It's configured to run on port 8000.
    *   The Web UI's `app.js` connects to `ws://localhost:8000/ws/...`. The `server.py` for the UI runs on port 8080 and proxies API calls, but WebSockets connect directly.
*   **Jobs Not Processing:**
    *   Check the logs for the `echo_worker` agent (formerly `worker`).
    *   Verify that the `pubsub` component is correctly configured and that messages are being published to the `job-queue` topic.
*   **State Not Persisting:**
    *   Check the `statestore` component configuration.
    *   Ensure Redis is accessible and functioning.
*   **Chat History Not Loading or Returns 404 ("Not Found"):**
    *   The Web UI (`web-ui/server.py`) proxies requests for `/api/chat/history/global` to the `chat` agent at `http://localhost:8002/chat/history/global`.
    *   If you see a 404 error in the browser's network tab for this request, it means the `chat` agent itself is returning a 404.
    *   Verify the `get_chat_history` function in `agents/chat/app.py` and how it handles the `session_id` "global". Ensure the endpoint `/chat/history/global` is correctly implemented and that data exists for this key in the statestore.
*   **Port Conflicts:**
    *   If any of the default ports (8000, 8001, 8002 for apps; 8080 for UI; Dapr default ports) are in use, you may need to adjust them in `dapr.yaml` for the apps, `web-ui/server.py` for the UI, or Dapr configurations.

## Utility Scripts

### Clearing All Chat Sessions

A script is provided to clear all chat session data (session metadata and conversation histories) from the Dapr state store (Redis).

To run this script:
1.  Ensure your Dapr environment is running (e.g., `dapr init` has been done and Redis is accessible).
2.  Execute the following command from the `multi-agent-system` root directory:
    ```bash
    dapr run --app-id clear-sessions-util -- python scripts/clear_sessions.py
    ```
    This command runs the `clear_sessions.py` script within a Dapr context, allowing it to connect to the configured state store.

## Advanced Features

Refer to `dapr_test/claude_plan.md` for information on:
*   Scaling Echo Workers (Example: `dapr run --app-id echo_worker-2 --app-port 8003 --dapr-http-port 3503 -- python agents/worker/app.py`)
*   Authentication (JWT implementation in coordinator)
*   Monitoring and Observability (Dapr tracing with Zipkin)
```

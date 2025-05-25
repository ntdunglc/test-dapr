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
│   ├── worker/
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
    Use the Dapr CLI to run all defined applications (coordinator, worker, chat) as specified in `dapr.yaml`. Execute this command from the `multi-agent-system` root directory:
    ```bash
    dapr run -f .
    ```
    This command will start the coordinator, worker, and chat agents, each with its own Dapr sidecar. You will see logs from Dapr and the individual applications in your terminal.

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
*   **WebSocket Connection Fails:**
    *   Ensure the coordinator agent (`appID: coordinator`) is running correctly (check Dapr logs). It's configured to run on port 8000.
    *   The Web UI's `app.js` connects to `ws://localhost:8000/ws/...`. The `server.py` for the UI runs on port 8080 and proxies API calls, but WebSockets connect directly.
*   **Jobs Not Processing:**
    *   Check the logs for the `worker` agent.
    *   Verify that the `pubsub` component is correctly configured and that messages are being published to the `job-queue` topic.
*   **State Not Persisting:**
    *   Check the `statestore` component configuration.
    *   Ensure Redis is accessible and functioning.
*   **Port Conflicts:**
    *   If any of the default ports (8000, 8001, 8002 for apps; 8080 for UI; Dapr default ports) are in use, you may need to adjust them in `dapr.yaml` for the apps, `web-ui/server.py` for the UI, or Dapr configurations.

## Advanced Features

Refer to `dapr_test/claude_plan.md` for information on:
*   Scaling Workers
*   Authentication (JWT implementation in coordinator)
*   Monitoring and Observability (Dapr tracing with Zipkin)
```

# justfile - Task runner for the multi-agent system

# Set the shell for executing recipes. -e: exit on error, -u: undefined variables are errors, -x: print commands, -c: execute command.
set shell := ["bash", "-euxc"]

# --- Variables ---
PROJECT_DIR := "multi-agent-system"

COORDINATOR_DIR := PROJECT_DIR + "/agents/coordinator"
CHAT_DIR := PROJECT_DIR + "/agents/chat"
WORKER_DIR := PROJECT_DIR + "/agents/worker" # Contains both actual echo (app.py) and LLM (agent.py) worker logic
WEBUI_DIR := PROJECT_DIR + "/web-ui"
SCRIPTS_DIR := PROJECT_DIR + "/scripts"

COMPONENTS_DIR := PROJECT_DIR + "/components"
DAPR_YAML_FILE := PROJECT_DIR + "/dapr.yaml"
# Dapr configuration file name (Dapr will look for .yaml extension if not provided)
# This refers to 'dapr-tracing-config.yaml' inside the COMPONENTS_DIR
DAPR_CONFIG_NAME := "dapr-tracing-config"


# --- Default Task ---
default:
    @echo "Available commands:"
    @just --list

# --- Installation ---
install-all: install-coordinator-req install-chat-req install-worker-req install-scripts-req install-gemini-test-req install-webui-req
    @echo "All Python dependencies installed."

install-coordinator-req:
    pip install -r {{COORDINATOR_DIR}}/requirements.txt

install-chat-req:
    pip install -r {{CHAT_DIR}}/requirements.txt

install-worker-req:
    pip install -r {{WORKER_DIR}}/requirements.txt

install-scripts-req:
    pip install -r {{SCRIPTS_DIR}}/requirements.txt

install-gemini-test-req:
    pip install -r {{SCRIPTS_DIR}}/test_gemini_requirements.txt

install-webui-req:
    pip install -r {{WEBUI_DIR}}/requirements.txt

# --- Run Dapr Services (Individually) ---
# These use app-ports and app-ids as defined in dapr.yaml where possible.
# Dapr HTTP/gRPC ports are assigned conventionally for individual runs.

run-coordinator:
    dapr run --app-id coordinator \
             --app-port 8000 \
             --dapr-http-port 3500 \
             --dapr-grpc-port 50001 \
             --components-path {{COMPONENTS_DIR}} \
             --config {{DAPR_CONFIG_NAME}} \
             --app-dir {{COORDINATOR_DIR}} \
             -- python -m uvicorn app:app --host 0.0.0.0 --port 8000 --reload

# This runs the LLM Worker (agents/worker/agent.py) using app-id 'echo_worker' as per dapr.yaml
run-llm-worker:
    dapr run --app-id echo_worker \
             --app-port 8001 \
             --dapr-http-port 3501 \
             --dapr-grpc-port 50002 \
             --components-path {{COMPONENTS_DIR}} \
             --config {{DAPR_CONFIG_NAME}} \
             --app-dir {{WORKER_DIR}} \
             -- python -m uvicorn agent:app --host 0.0.0.0 --port 8001 --reload

run-chat:
    dapr run --app-id chat \
             --app-port 8002 \
             --dapr-http-port 3502 \
             --dapr-grpc-port 50003 \
             --components-path {{COMPONENTS_DIR}} \
             --config {{DAPR_CONFIG_NAME}} \
             --app-dir {{CHAT_DIR}} \
             -- python -m uvicorn app:app --host 0.0.0.0 --port 8002 --reload

# This runs the actual Echo Worker (agents/worker/app.py)
# It uses a new app-id and app-port to distinguish from the LLM worker.
run-actual-echo-worker:
    dapr run --app-id actual-echo-worker \
             --app-port 8003 \
             --dapr-http-port 3503 \
             --dapr-grpc-port 50004 \
             --components-path {{COMPONENTS_DIR}} \
             --config {{DAPR_CONFIG_NAME}} \
             --app-dir {{WORKER_DIR}} \
             -- python -m uvicorn app:app --host 0.0.0.0 --port 8003 --reload


# --- Run Web UI ---
run-webui:
    @echo "Starting Web UI. Access it at http://localhost:8050"
    cd {{WEBUI_DIR}} && python -m uvicorn server:app --host 0.0.0.0 --port 8050 --reload

# --- Dapr Multi-App Run (using dapr.yaml) ---
run-dapr-all:
    dapr run --log-level debug -f {{DAPR_YAML_FILE}}

stop-dapr-all:
    dapr stop -f {{DAPR_YAML_FILE}}
    @echo "Attempted to stop all Dapr applications defined in {{DAPR_YAML_FILE}}."
    @echo "You may need to manually stop individual 'dapr run' processes if any were started separately."

# --- Utility Scripts ---
script-clear-sessions:
    python {{SCRIPTS_DIR}}/clear_sessions.py

script-get-conversation-state session_id="":
    {{ if (session_id == "") }}
        @echo "Usage: just script-get-conversation-state <session_id>"
        @exit 1
    {{ else }}
        python {{SCRIPTS_DIR}}/get_conversation_state.py {{session_id}}
    {{ endif }}

script-test-gemini:
    python {{SCRIPTS_DIR}}/test_gemini_api.py

# --- Dapr Management ---
dapr-dashboard:
    dapr dashboard -p 9909 # Default Dapr dashboard port is 8080, using 9909 to avoid common conflicts

dapr-list:
    dapr list

# Logs for services as defined in dapr.yaml or individual run commands
dapr-logs-coordinator:
    dapr logs --app-id coordinator -f

dapr-logs-chat:
    dapr logs --app-id chat -f

# Logs for the LLM worker (which has app-id 'echo_worker' in dapr.yaml and run-llm-worker)
dapr-logs-llm-worker:
    dapr logs --app-id echo_worker -f

# Logs for the actual Echo worker (if run individually via 'run-actual-echo-worker')
dapr-logs-actual-echo-worker:
    dapr logs --app-id actual-echo-worker -f

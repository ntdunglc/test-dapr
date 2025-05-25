# This file makes the 'worker' directory a Python package.
# It can be used to expose elements from the package, e.g., the agent.py's app or root_agent.
# For ADK discovery or Uvicorn, often it's enough for this file to exist.
# Following ADK quickstart:
from . import agent

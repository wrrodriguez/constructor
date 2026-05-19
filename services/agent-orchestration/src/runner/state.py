# src/runner/state.py
from typing import Annotated, TypedDict
from langchain_core.messages import BaseMessage
from langgraph.graph.message import add_messages


class AgentState(TypedDict):
    messages: Annotated[list[BaseMessage], add_messages]
    iterations: int
    max_iterations: int
    context: dict
    tools_allowed: list[str]
    task_id: str
    tenant_id: str

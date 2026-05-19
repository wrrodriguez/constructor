# src/runner/nodes.py
from langchain_core.messages import AIMessage, ToolMessage
from langchain_core.runnables import RunnableConfig
from langgraph.graph import END
from src.runner.state import AgentState


async def call_model_node(state: AgentState, config: RunnableConfig) -> dict:
    model = config["configurable"]["model"]
    tools = config["configurable"]["tools"]
    bound = model.bind_tools([t.as_openai_tool() for t in tools])
    response = await bound.ainvoke(state["messages"])
    return {
        "messages": [response],
        "iterations": state["iterations"] + 1,
    }


async def execute_tool_node(state: AgentState, config: RunnableConfig) -> dict:
    registry = config["configurable"]["registry"]
    tenant_context = {"tenant_id": state["tenant_id"]}
    last_msg: AIMessage = state["messages"][-1]
    results = []

    for tc in last_msg.tool_calls:
        tool = registry.get(tc["name"])
        if tool is None:
            content = f"Error: tool '{tc['name']}' not registered"
        else:
            try:
                out = await tool.execute(tc["args"], tenant_context)
                content = str(out)
            except Exception as exc:
                content = f"Error: {exc}"
        results.append(ToolMessage(content=content, tool_call_id=tc["id"]))

    return {"messages": results}


def should_continue(state: AgentState) -> str:
    last = state["messages"][-1]
    if (
        isinstance(last, AIMessage)
        and last.tool_calls
        and state["iterations"] < state["max_iterations"]
    ):
        return "execute_tool"
    return END

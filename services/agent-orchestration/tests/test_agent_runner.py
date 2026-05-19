# tests/test_agent_runner.py
import pytest
from langchain_core.messages import AIMessage
from unittest.mock import MagicMock, AsyncMock
from src.runner.agent_runner import AgentRunner
from src.tools.registry import ToolRegistry
from src.tools.base import BaseTool


@pytest.mark.asyncio
async def test_runner_happy_path_no_tools(sample_task, mock_model_router, empty_registry):
    """LLM responde sin tool calls → completed en 1 iteración."""
    final_msg = AIMessage(content="Analysis complete: no issues found.")
    mock_model_router.get_model.return_value = make_mock_model_from(final_msg)

    runner = AgentRunner(mock_model_router, empty_registry)
    result = await runner.run(sample_task)

    assert result.status == "completed"
    assert result.output["text"] == "Analysis complete: no issues found."
    assert result.iterations == 1
    assert result.error is None
    assert result.execution_id == sample_task.execution_id


@pytest.mark.asyncio
async def test_runner_with_one_tool_call(sample_task, mock_model_router):
    """LLM hace un tool call, luego responde → completed en 2 iteraciones."""
    tool_call_msg = AIMessage(
        content="",
        tool_calls=[{
            "id": "call_abc",
            "name": "fake_tool",
            "args": {"value": "hello"},
        }],
    )
    final_msg = AIMessage(content="Tool returned hello. Done.")

    mock_tool = MagicMock(spec=BaseTool)
    mock_tool.name = "fake_tool"
    mock_tool.as_openai_tool.return_value = {
        "type": "function",
        "function": {"name": "fake_tool", "description": "...", "parameters": {}},
    }
    mock_tool.execute = AsyncMock(return_value={"result": "hello"})

    registry = ToolRegistry()
    registry.register(mock_tool)

    call_count = 0
    async def side_effect(msgs, **kw):
        nonlocal call_count
        call_count += 1
        return tool_call_msg if call_count == 1 else final_msg

    bound = MagicMock()
    bound.ainvoke = AsyncMock(side_effect=side_effect)
    mock_model = MagicMock()
    mock_model.bind_tools = MagicMock(return_value=bound)
    mock_model_router.get_model.return_value = mock_model

    task_with_tools = sample_task.model_copy(update={"tools_allowed": ["fake_tool"]})
    runner = AgentRunner(mock_model_router, registry)
    result = await runner.run(task_with_tools)

    assert result.status == "completed"
    assert result.iterations == 2
    mock_tool.execute.assert_called_once_with({"value": "hello"}, {"tenant_id": str(sample_task.tenant_id)})


@pytest.mark.asyncio
async def test_runner_timeout(sample_task, mock_model_router, empty_registry):
    """Timeout devuelve status='timeout'."""
    import asyncio

    async def slow_response(msgs, **kw):
        await asyncio.sleep(10)
        return AIMessage(content="never")

    bound = MagicMock()
    bound.ainvoke = AsyncMock(side_effect=slow_response)
    mock_model = MagicMock()
    mock_model.bind_tools = MagicMock(return_value=bound)
    mock_model_router.get_model.return_value = mock_model

    short_task = sample_task.model_copy(update={"timeout_seconds": 1})
    runner = AgentRunner(mock_model_router, empty_registry)
    result = await runner.run(short_task)

    assert result.status == "timeout"
    assert result.error is not None


def make_mock_model_from(response: AIMessage):
    bound = MagicMock()
    bound.ainvoke = AsyncMock(return_value=response)
    model = MagicMock()
    model.bind_tools = MagicMock(return_value=bound)
    return model

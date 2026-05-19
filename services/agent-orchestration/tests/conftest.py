# tests/conftest.py
import pytest
import uuid
from unittest.mock import AsyncMock, MagicMock
from langchain_core.messages import AIMessage
from src.events import AgentTask
from src.router.model_router import ModelRouter
from src.tools.registry import ToolRegistry


def make_mock_model(responses: list[AIMessage]) -> MagicMock:
    """Mock ChatModel que devuelve AIMessages predefinidos en orden."""
    responses_iter = iter(responses)

    bound = MagicMock()
    bound.ainvoke = AsyncMock(side_effect=lambda msgs, **kw: next(responses_iter))

    model = MagicMock()
    model.bind_tools = MagicMock(return_value=bound)
    return model


@pytest.fixture
def sample_task() -> AgentTask:
    return AgentTask(
        task_id=uuid.uuid4(),
        execution_id=uuid.uuid4(),
        tenant_id=uuid.uuid4(),
        step_id="step_1",
        task_type="default",
        prompt="Analyze this code snippet",
        context={"code": "def foo(): return 42"},
        tools_allowed=[],
        timeout_seconds=30,
        max_iterations=5,
    )


@pytest.fixture
def mock_model_router() -> MagicMock:
    router = MagicMock(spec=ModelRouter)
    router.last_model_used = "claude-sonnet-4-6"
    return router


@pytest.fixture
def empty_registry() -> ToolRegistry:
    return ToolRegistry()

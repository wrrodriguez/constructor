# tests/test_executor.py
import pytest
import uuid
from unittest.mock import AsyncMock, patch, MagicMock
from sqlalchemy.ext.asyncio import AsyncSession
from src.executions.condition import evaluate_condition
from src.executions.transform import apply_transform


# --- condition ---

def test_condition_gt_true():
    config = {
        "expression": "score",
        "operator": ">",
        "value": 0.8,
        "branches": {"true": "step_approval", "false": "step_notify"},
    }
    assert evaluate_condition(config, {"score": 0.9}) == "step_approval"


def test_condition_gt_false():
    config = {
        "expression": "score",
        "operator": ">",
        "value": 0.8,
        "branches": {"true": "step_approval", "false": "step_notify"},
    }
    assert evaluate_condition(config, {"score": 0.5}) == "step_notify"


def test_condition_eq():
    config = {
        "expression": "status",
        "operator": "==",
        "value": "approved",
        "branches": {"true": "step_emit", "false": "step_review"},
    }
    assert evaluate_condition(config, {"status": "approved"}) == "step_emit"


def test_condition_nested_jmespath():
    config = {
        "expression": "steps.agent_1.risk_score",
        "operator": ">=",
        "value": 0.7,
        "branches": {"true": "high_risk", "false": "low_risk"},
    }
    context = {"steps": {"agent_1": {"risk_score": 0.75}}}
    assert evaluate_condition(config, context) == "high_risk"


def test_condition_in_operator():
    config = {
        "expression": "category",
        "operator": "in",
        "value": ["A", "B", "C"],
        "branches": {"true": "valid", "false": "invalid"},
    }
    assert evaluate_condition(config, {"category": "B"}) == "valid"
    assert evaluate_condition(config, {"category": "D"}) == "invalid"


# --- transform ---

def test_transform_simple_mapping():
    config = {"output.summary": "agent_1.text"}
    context = {"agent_1": {"text": "hello world"}}
    result = apply_transform(config, context)
    assert result["output"]["summary"] == "hello world"
    assert result["agent_1"]["text"] == "hello world"  # original untouched


def test_transform_multiple_fields():
    config = {
        "report.title": "input.name",
        "report.score": "analysis.risk_score",
    }
    context = {"input": {"name": "Policy #123"}, "analysis": {"risk_score": 0.42}}
    result = apply_transform(config, context)
    assert result["report"]["title"] == "Policy #123"
    assert result["report"]["score"] == 0.42


def test_transform_missing_source_sets_none():
    config = {"output.value": "nonexistent.path"}
    context = {}
    result = apply_transform(config, context)
    assert result["output"]["value"] is None


# --- executor ---

@pytest.mark.asyncio
async def test_executor_dispatches_agent_step():
    """Cuando hay un agent step, dispatch_agent_step es llamado."""
    from src.executions.executor import execute_workflow

    mock_dispatch = AsyncMock(return_value={"text": "analysis result"})
    with patch("src.executions.executor.dispatch_agent_step", mock_dispatch):
        mock_execution = MagicMock()
        mock_execution.id = uuid.uuid4()
        mock_execution.tenant_id = uuid.uuid4()
        mock_execution.context = {}
        mock_execution.status = "pending"
        mock_execution.current_step_id = None

        mock_definition = MagicMock()
        mock_definition.steps = [
            {
                "id": "s1",
                "type": "agent",
                "config": {"task_type": "default", "prompt": "analyze"},
                "next": None,
                "timeout_seconds": 60,
            }
        ]

        async def mock_get(model, pk):
            if "ProcessExecution" in str(model):
                return mock_execution
            return mock_definition

        mock_db = AsyncMock(spec=AsyncSession)
        mock_db.get = AsyncMock(side_effect=mock_get)
        mock_db.commit = AsyncMock()

        await execute_workflow(
            mock_execution.id,
            uuid.uuid4(),
            mock_execution.tenant_id,
            mock_db,
        )

    mock_dispatch.assert_called_once()
    assert mock_execution.status == "completed"

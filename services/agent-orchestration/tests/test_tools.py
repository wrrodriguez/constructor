# tests/test_tools.py
import pytest
from src.tools.registry import ToolRegistry
from src.tools.base import BaseTool


class FakeTool(BaseTool):
    name = "fake_tool"
    description = "A fake tool for testing"
    input_schema = {
        "type": "object",
        "properties": {"value": {"type": "string"}},
        "required": ["value"],
    }

    async def execute(self, inputs: dict, tenant_context: dict) -> dict:
        return {"result": inputs["value"]}


def test_registry_register_and_get():
    registry = ToolRegistry()
    tool = FakeTool()
    registry.register(tool)
    assert registry.get("fake_tool") is tool


def test_registry_get_unknown_returns_none():
    registry = ToolRegistry()
    assert registry.get("nonexistent") is None


def test_registry_list_names():
    registry = ToolRegistry()
    registry.register(FakeTool())
    assert "fake_tool" in registry.list_names()


def test_tool_as_openai_spec():
    tool = FakeTool()
    spec = tool.as_openai_tool()
    assert spec["type"] == "function"
    assert spec["function"]["name"] == "fake_tool"
    assert "parameters" in spec["function"]


@pytest.mark.asyncio
async def test_tool_execute():
    tool = FakeTool()
    result = await tool.execute({"value": "hello"}, {"tenant_id": "t1"})
    assert result == {"result": "hello"}


# --- http_generic ---

import ipaddress
from unittest.mock import AsyncMock, patch, MagicMock
from src.tools.http_generic import HttpGenericTool
from src.tools.sql_query import SqlQueryTool, _is_select_only


@pytest.mark.asyncio
async def test_http_generic_blocks_private_ip():
    tool = HttpGenericTool()
    with pytest.raises(ValueError, match="private"):
        await tool.execute(
            {"url": "http://192.168.1.1/secret", "method": "GET", "headers": {}},
            {"tenant_id": "t1"},
        )


@pytest.mark.asyncio
async def test_http_generic_blocks_localhost():
    tool = HttpGenericTool()
    with pytest.raises(ValueError, match="private"):
        await tool.execute(
            {"url": "http://127.0.0.1/admin", "method": "GET", "headers": {}},
            {"tenant_id": "t1"},
        )


@pytest.mark.asyncio
async def test_http_generic_makes_request():
    tool = HttpGenericTool()
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"ok": True}
    mock_response.headers = {"content-type": "application/json"}

    with patch("httpx.AsyncClient") as MockClient:
        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client.request = AsyncMock(return_value=mock_response)
        MockClient.return_value = mock_client

        result = await tool.execute(
            {"url": "http://example.com/api", "method": "GET", "headers": {}},
            {"tenant_id": "t1"},
        )

    assert result["status_code"] == 200
    assert result["body"] == {"ok": True}


# --- sql_query ---

def test_select_is_allowed():
    assert _is_select_only("SELECT id, name FROM users WHERE active = true") is True


def test_select_with_cte_is_allowed():
    assert _is_select_only("WITH cte AS (SELECT 1) SELECT * FROM cte") is True


def test_insert_is_rejected():
    assert _is_select_only("INSERT INTO users (email) VALUES ('x@y.com')") is False


def test_update_is_rejected():
    assert _is_select_only("UPDATE users SET active = false") is False


def test_delete_is_rejected():
    assert _is_select_only("DELETE FROM users") is False


def test_drop_is_rejected():
    assert _is_select_only("DROP TABLE users") is False


@pytest.mark.asyncio
async def test_sql_query_rejects_dml():
    tool = SqlQueryTool()
    with pytest.raises(ValueError, match="SELECT"):
        await tool.execute(
            {"query": "DELETE FROM users", "params": {}},
            {"tenant_id": "t1"},
        )

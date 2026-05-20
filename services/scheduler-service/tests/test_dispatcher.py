# tests/test_dispatcher.py
import uuid
import pytest
from unittest.mock import AsyncMock, MagicMock, patch


async def test_dispatch_acquires_lock_calls_api_releases_lock():
    from src.dispatcher import dispatch

    wf_id = uuid.uuid4()
    tenant_id = uuid.uuid4()

    mock_redis = AsyncMock()
    mock_redis.set = AsyncMock(return_value=True)  # lock acquired
    mock_redis.delete = AsyncMock()

    mock_response = MagicMock()
    mock_response.raise_for_status = MagicMock()

    mock_client = AsyncMock()
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)
    mock_client.post = AsyncMock(return_value=mock_response)

    with patch("src.dispatcher.httpx.AsyncClient", return_value=mock_client):
        await dispatch(wf_id, tenant_id, mock_redis)

    mock_redis.set.assert_called_once()
    mock_client.post.assert_called_once()
    mock_redis.delete.assert_called_once()


async def test_dispatch_skips_when_lock_not_acquired():
    from src.dispatcher import dispatch

    wf_id = uuid.uuid4()
    tenant_id = uuid.uuid4()

    mock_redis = AsyncMock()
    mock_redis.set = AsyncMock(return_value=None)  # lock NOT acquired (None = NX failed)
    mock_redis.delete = AsyncMock()

    with patch("src.dispatcher.httpx.AsyncClient") as mock_client_cls:
        await dispatch(wf_id, tenant_id, mock_redis)

    mock_client_cls.assert_not_called()
    mock_redis.delete.assert_not_called()


async def test_dispatch_releases_lock_even_on_api_failure():
    from src.dispatcher import dispatch

    wf_id = uuid.uuid4()
    tenant_id = uuid.uuid4()

    mock_redis = AsyncMock()
    mock_redis.set = AsyncMock(return_value=True)
    mock_redis.delete = AsyncMock()

    mock_client = AsyncMock()
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)
    mock_client.post = AsyncMock(side_effect=Exception("connection refused"))

    with patch("src.dispatcher.httpx.AsyncClient", return_value=mock_client):
        await dispatch(wf_id, tenant_id, mock_redis)  # must not raise

    mock_redis.delete.assert_called_once()

# tests/test_consumer.py
import asyncio
import json
import pytest
import pytest_asyncio
from unittest.mock import AsyncMock, patch
from src.consumer.redis_consumer import StatusConsumer, STATUS_STREAM


@pytest.mark.asyncio
async def test_consumer_emits_execution_update_to_room(redis_client):
    """Publicar en el stream → sio.emit() con el evento correcto a la room."""
    event = {
        "execution_id": "3f4e7a10-0000-0000-0000-000000000001",
        "tenant_id": "aaaaaaaa-0000-0000-0000-000000000001",
        "status": "completed",
        "current_step_id": None,
        "context": {"result": "ok"},
        "timestamp": "2026-05-19T10:00:00+00:00",
    }
    await redis_client.xadd(STATUS_STREAM, {"data": json.dumps(event)})

    with patch("src.consumer.redis_consumer.sio") as mock_sio:
        mock_sio.emit = AsyncMock()
        consumer = StatusConsumer(redis_client)
        try:
            await asyncio.wait_for(consumer.start(), timeout=3)
        except asyncio.TimeoutError:
            pass

    mock_sio.emit.assert_called_once()
    call_args = mock_sio.emit.call_args
    assert call_args.args[0] == "execution_update"
    assert call_args.kwargs["room"] == "3f4e7a10-0000-0000-0000-000000000001"
    payload = call_args.args[1]
    assert payload["status"] == "completed"
    assert payload["execution_id"] == "3f4e7a10-0000-0000-0000-000000000001"


@pytest.mark.asyncio
async def test_consumer_acks_message_even_on_emit_failure(redis_client):
    """Si sio.emit falla, el mensaje igual se ackea para no bloquear el stream."""
    event = {
        "execution_id": "3f4e7a10-0000-0000-0000-000000000002",
        "tenant_id": "aaaaaaaa-0000-0000-0000-000000000001",
        "status": "failed",
        "current_step_id": None,
        "context": {},
        "timestamp": "2026-05-19T10:00:00+00:00",
    }
    await redis_client.xadd(STATUS_STREAM, {"data": json.dumps(event)})

    with patch("src.consumer.redis_consumer.sio") as mock_sio:
        mock_sio.emit = AsyncMock(side_effect=RuntimeError("emit failed"))
        consumer = StatusConsumer(redis_client)
        try:
            await asyncio.wait_for(consumer.start(), timeout=3)
        except asyncio.TimeoutError:
            pass

    # El stream no debe tener mensajes pendientes (todos ackados)
    pending = await redis_client.xpending_range(
        STATUS_STREAM, "websocket-service", "-", "+", 10
    )
    assert len(pending) == 0


@pytest.mark.asyncio
async def test_consumer_skips_malformed_message(redis_client):
    """Mensajes malformados se ackean y no detienen el consumer."""
    await redis_client.xadd(STATUS_STREAM, {"data": "not-valid-json"})

    with patch("src.consumer.redis_consumer.sio") as mock_sio:
        mock_sio.emit = AsyncMock()
        consumer = StatusConsumer(redis_client)
        try:
            await asyncio.wait_for(consumer.start(), timeout=3)
        except asyncio.TimeoutError:
            pass

    # sio.emit no fue llamado (mensaje ignorado)
    mock_sio.emit.assert_not_called()

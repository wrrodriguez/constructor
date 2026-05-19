# tests/test_socket.py
import pytest
from unittest.mock import patch, AsyncMock, MagicMock

# Importing main registers the handlers on sio
import src.main
from src.main import connect, disconnect, join_execution, leave_execution
from src.socket_manager import sio


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _session_ctx(store: dict):
    """Build a minimal async context manager that mimics sio.session(sid)."""
    class _Ctx:
        async def __aenter__(self):
            return store

        async def __aexit__(self, *args):
            pass

    return _Ctx()


# ---------------------------------------------------------------------------
# connect tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_connect_valid_token_accepted(valid_token):
    store: dict = {}
    with patch.object(sio, "session", return_value=_session_ctx(store)):
        result = await connect("sid-1", {}, {"token": valid_token})
    # Handler returns None on success (no explicit return → accepted)
    assert result is None
    assert store["tenant_id"] == "tenant-111"
    assert store["user_id"] == "user-222"


@pytest.mark.asyncio
async def test_connect_invalid_token_rejected():
    with pytest.raises(ConnectionRefusedError, match="Invalid token"):
        await connect("sid-2", {}, {"token": "not-a-token"})


@pytest.mark.asyncio
async def test_connect_expired_token_rejected(expired_token):
    with pytest.raises(ConnectionRefusedError, match="Token expired"):
        await connect("sid-3", {}, {"token": expired_token})


@pytest.mark.asyncio
async def test_connect_missing_token_rejected():
    with pytest.raises(ConnectionRefusedError, match="Missing token"):
        await connect("sid-4", {}, {})


@pytest.mark.asyncio
async def test_connect_no_auth_rejected():
    with pytest.raises(ConnectionRefusedError, match="Missing token"):
        await connect("sid-5", {}, None)


# ---------------------------------------------------------------------------
# join_execution tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_join_execution_own_tenant(valid_token):
    store = {"tenant_id": "tenant-111", "user_id": "user-222"}
    emitted = []

    async def _fake_emit(event, data, to=None, room=None, **kw):
        emitted.append({"name": event, "data": data, "to": to})

    enter_room_mock = MagicMock()

    with (
        patch.object(sio, "session", return_value=_session_ctx(store)),
        patch.object(sio, "emit", side_effect=_fake_emit),
        patch.object(sio, "enter_room", new=enter_room_mock),
        patch("src.main._get_execution_tenant", new=AsyncMock(return_value="tenant-111")),
    ):
        await join_execution("sid-1", {"execution_id": "exec-abc"})

    error_events = [e for e in emitted if e["name"] == "error"]
    assert error_events == []
    enter_room_mock.assert_called_once_with("sid-1", "exec-abc")


@pytest.mark.asyncio
async def test_join_execution_other_tenant_rejected(valid_token):
    store = {"tenant_id": "tenant-111", "user_id": "user-222"}
    emitted = []

    async def _fake_emit(event, data, to=None, room=None, **kw):
        emitted.append({"name": event, "args": [data], "to": to})

    enter_room_mock = MagicMock()

    with (
        patch.object(sio, "session", return_value=_session_ctx(store)),
        patch.object(sio, "emit", side_effect=_fake_emit),
        patch.object(sio, "enter_room", new=enter_room_mock),
        patch("src.main._get_execution_tenant", new=AsyncMock(return_value="tenant-OTHER")),
    ):
        await join_execution("sid-1", {"execution_id": "exec-abc"})

    errors = [e for e in emitted if e["name"] == "error"]
    assert len(errors) == 1
    assert errors[0]["args"][0]["code"] == 403
    enter_room_mock.assert_not_called()


@pytest.mark.asyncio
async def test_join_execution_not_found_rejected(valid_token):
    store = {"tenant_id": "tenant-111", "user_id": "user-222"}
    emitted = []

    async def _fake_emit(event, data, to=None, room=None, **kw):
        emitted.append({"name": event, "args": [data], "to": to})

    enter_room_mock = MagicMock()

    with (
        patch.object(sio, "session", return_value=_session_ctx(store)),
        patch.object(sio, "emit", side_effect=_fake_emit),
        patch.object(sio, "enter_room", new=enter_room_mock),
        patch("src.main._get_execution_tenant", new=AsyncMock(return_value=None)),
    ):
        await join_execution("sid-1", {"execution_id": "exec-nonexistent"})

    errors = [e for e in emitted if e["name"] == "error"]
    assert len(errors) == 1
    assert errors[0]["args"][0]["code"] == 403
    enter_room_mock.assert_not_called()


@pytest.mark.asyncio
async def test_join_execution_missing_execution_id():
    store = {"tenant_id": "tenant-111", "user_id": "user-222"}
    emitted = []

    async def _fake_emit(event, data, to=None, room=None, **kw):
        emitted.append({"name": event, "args": [data], "to": to})

    enter_room_mock = MagicMock()

    with (
        patch.object(sio, "session", return_value=_session_ctx(store)),
        patch.object(sio, "emit", side_effect=_fake_emit),
        patch.object(sio, "enter_room", new=enter_room_mock),
    ):
        await join_execution("sid-1", {})

    errors = [e for e in emitted if e["name"] == "error"]
    assert len(errors) == 1
    assert errors[0]["args"][0]["code"] == 400


# ---------------------------------------------------------------------------
# room broadcast test
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_two_clients_same_room_both_receive():
    """Two sids in the same room both appear as get_participants result."""
    ns = "/"
    room = "exec-broadcast-test"

    # Connect both sids to the manager (public API); returns the generated sid
    sid_a = await sio.manager.connect("eio-broadcast-a", ns)
    sid_b = await sio.manager.connect("eio-broadcast-b", ns)
    await sio.enter_room(sid_a, room)
    await sio.enter_room(sid_b, room)
    try:
        assert room in sio.rooms(sid_a)
        assert room in sio.rooms(sid_b)
        participants = {sid for sid, _ in sio.manager.get_participants(ns, room)}
        assert sid_a in participants
        assert sid_b in participants
    finally:
        await sio.leave_room(sid_a, room)
        await sio.leave_room(sid_b, room)
        await sio.manager.disconnect(sid_a, ns)
        await sio.manager.disconnect(sid_b, ns)

# src/executions/dispatcher.py
import asyncio
import uuid

# Module-level shared state: str(execution_id) -> asyncio.Future
_pending: dict[str, asyncio.Future] = {}


def register_pending(execution_id: uuid.UUID) -> asyncio.Future:
    """Register a Future that will be resolved when agent.result.ready arrives."""
    loop = asyncio.get_event_loop()
    future: asyncio.Future = loop.create_future()
    _pending[str(execution_id)] = future
    return future


def resolve_pending(execution_id: str, output: dict) -> bool:
    """Called by Redis consumer. Returns True if a Future was waiting."""
    future = _pending.pop(execution_id, None)
    if future and not future.done():
        future.set_result(output)
        return True
    return False


def reject_pending(execution_id: str, error: str) -> bool:
    """Called by Redis consumer on agent failure. Returns True if resolved."""
    future = _pending.pop(execution_id, None)
    if future and not future.done():
        future.set_exception(RuntimeError(error))
        return True
    return False

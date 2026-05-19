# tests/test_consumer.py
import asyncio
import pytest
import pytest_asyncio
import uuid
from unittest.mock import AsyncMock, MagicMock
from testcontainers.redis import RedisContainer
from redis.asyncio import Redis
from src.events import AgentTask, AgentResult
from src.consumer.redis_consumer import RedisConsumer, TASK_STREAM
from src.publisher.result_publisher import ResultPublisher, RESULT_STREAM


@pytest.fixture(scope="module")
def redis_container():
    with RedisContainer("redis:7-alpine") as r:
        yield r


@pytest_asyncio.fixture(scope="module")
async def redis_client(redis_container):
    client = Redis(
        host=redis_container.get_container_host_ip(),
        port=int(redis_container.get_exposed_port(6379)),
        decode_responses=True,
    )
    yield client
    await client.aclose()


@pytest.mark.asyncio
async def test_consumer_processes_task_and_publishes_result(redis_client):
    task = AgentTask(
        task_id=uuid.uuid4(), execution_id=uuid.uuid4(),
        tenant_id=uuid.uuid4(), step_id="step_1",
        task_type="default", prompt="say hello",
        context={}, tools_allowed=[],
    )
    expected_result = AgentResult(
        task_id=task.task_id, execution_id=task.execution_id,
        tenant_id=task.tenant_id, step_id=task.step_id,
        status="completed", output={"text": "hello"},
        tokens_used=10, model_used="claude-sonnet-4-6",
        iterations=1, error=None,
    )

    mock_runner = MagicMock()
    mock_runner.run = AsyncMock(return_value=expected_result)

    mock_long_term = MagicMock()
    mock_long_term.write = AsyncMock()

    publisher = ResultPublisher(redis_client)
    consumer = RedisConsumer(redis_client, mock_runner, publisher, mock_long_term)

    # Publicar task en el stream
    await redis_client.xadd(TASK_STREAM, {"data": task.model_dump_json()})

    # Correr consumer por 3 segundos para procesar el mensaje
    try:
        await asyncio.wait_for(consumer.start(), timeout=3)
    except asyncio.TimeoutError:
        pass

    # Verificar que se publicó el resultado en el stream de salida
    results = await redis_client.xread({RESULT_STREAM: "0"}, count=10)
    assert results, "No results published to result stream"
    _stream, entries = results[0]
    assert len(entries) >= 1
    published = AgentResult.model_validate_json(entries[0][1]["data"])
    assert published.execution_id == task.execution_id
    assert published.status == "completed"
    mock_long_term.write.assert_called_once()

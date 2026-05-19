# src/worker.py
import asyncio
import logging
import uvicorn
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import create_async_engine
from src.config import settings
from src.main import app
from src.runner.agent_runner import AgentRunner
from src.router.model_router import ModelRouter
from src.tools.registry import ToolRegistry
from src.tools.http_generic import HttpGenericTool
from src.tools.sql_query import SqlQueryTool
from src.memory.long_term import LongTermMemory
from src.publisher.result_publisher import ResultPublisher
from src.consumer.redis_consumer import RedisConsumer

logging.basicConfig(level=logging.INFO)


async def main() -> None:
    redis = Redis.from_url(settings.redis_url, decode_responses=True)
    engine = create_async_engine(settings.database_url)

    registry = ToolRegistry()
    registry.register(HttpGenericTool())
    registry.register(SqlQueryTool())

    model_router = ModelRouter()
    runner = AgentRunner(model_router, registry)
    publisher = ResultPublisher(redis)
    long_term = LongTermMemory(engine)
    consumer = RedisConsumer(redis, runner, publisher, long_term)

    server = uvicorn.Server(uvicorn.Config(app, host="0.0.0.0", port=8001, log_level="info"))

    await asyncio.gather(server.serve(), consumer.start())


if __name__ == "__main__":
    asyncio.run(main())

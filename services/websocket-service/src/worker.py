# src/worker.py
import asyncio
import logging
import uvicorn
from redis.asyncio import Redis
from src.config import settings
from src.main import app
from src.consumer.redis_consumer import StatusConsumer

logging.basicConfig(level=logging.INFO)


async def main() -> None:
    redis = Redis.from_url(settings.redis_url, decode_responses=True)
    consumer = StatusConsumer(redis)
    server = uvicorn.Server(
        uvicorn.Config(app, host="0.0.0.0", port=settings.port, log_level="info")  # nosec B104
    )
    await asyncio.gather(server.serve(), consumer.start())


if __name__ == "__main__":
    asyncio.run(main())

# tests/conftest.py
import pytest
from unittest.mock import AsyncMock, MagicMock
from apscheduler.schedulers.asyncio import AsyncIOScheduler


@pytest.fixture
def scheduler():
    s = AsyncIOScheduler()
    return s


@pytest.fixture
def mock_engine():
    return AsyncMock()

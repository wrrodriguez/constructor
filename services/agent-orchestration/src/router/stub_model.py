# src/router/stub_model.py
"""
Stub LLM for local development — responds without calling any external API.
Activated when USE_STUB_MODEL=true in the environment.
"""
from __future__ import annotations

from typing import Any
from langchain_core.language_models import BaseChatModel
from langchain_core.messages import BaseMessage, AIMessage
from langchain_core.outputs import ChatGeneration, ChatResult


_STUB_RESPONSE = (
    "**Análisis de póliza (stub)**\n\n"
    "Este es un modelo stub para desarrollo local. "
    "La póliza recibida ha sido procesada correctamente. "
    "En producción este campo contendrá el análisis real generado por el LLM."
)


class StubChatModel(BaseChatModel):
    """Fake chat model that returns a fixed response. No API calls."""

    model_name: str = "stub"

    @property
    def _llm_type(self) -> str:
        return "stub"

    def _generate(
        self,
        messages: list[BaseMessage],
        stop: list[str] | None = None,
        **kwargs: Any,
    ) -> ChatResult:
        msg = AIMessage(content=_STUB_RESPONSE)
        return ChatResult(generations=[ChatGeneration(message=msg)])

    async def _agenerate(
        self,
        messages: list[BaseMessage],
        stop: list[str] | None = None,
        **kwargs: Any,
    ) -> ChatResult:
        return self._generate(messages, stop, **kwargs)

    def bind_tools(self, tools: list, **kwargs: Any) -> "StubChatModel":
        """No-op: stub ignores tools but must support the call signature."""
        return self

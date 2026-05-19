# src/router/model_router.py
from langchain_core.language_models import BaseChatModel
from src.config import settings

ROUTING_TABLE: dict[str, str] = {
    "code_analysis":  "gemini-2.0-flash",
    "security_scan":  "gemini-2.0-flash",
    "summarization":  "gemini-2.0-flash",
    "ocr_extraction": "gemini-2.0-flash",
    "default":        "gemini-2.0-flash",
}

FALLBACK_CHAIN: list[str] = ["gemini-2.0-flash", "gemini-2.0-flash-lite", "gemini-1.5-flash-latest"]


class ModelRouter:
    def __init__(self):
        self.last_model_used: str = ""

    def _resolve_model_id(self, task_type: str, model_override: str | None) -> str:
        return model_override or ROUTING_TABLE.get(task_type, ROUTING_TABLE["default"])

    def get_model(self, task_type: str, model_override: str | None = None) -> BaseChatModel:
        model_id = self._resolve_model_id(task_type, model_override)
        self.last_model_used = model_id
        return self._instantiate(model_id)

    def _instantiate(self, model_id: str) -> BaseChatModel:
        if settings.use_stub_model or model_id == "stub":
            from src.router.stub_model import StubChatModel
            return StubChatModel()
        if model_id.startswith("claude"):
            from langchain_anthropic import ChatAnthropic
            return ChatAnthropic(model=model_id, api_key=settings.anthropic_api_key)
        if "gpt" in model_id or model_id.startswith("o1") or model_id.startswith("o3"):
            from langchain_openai import ChatOpenAI
            return ChatOpenAI(model=model_id, api_key=settings.openai_api_key)
        if "gemini" in model_id:
            from langchain_google_genai import ChatGoogleGenerativeAI
            return ChatGoogleGenerativeAI(model=model_id, google_api_key=settings.google_api_key)
        raise ValueError(f"Unsupported model id: {model_id}")

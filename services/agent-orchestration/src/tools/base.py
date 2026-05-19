# src/tools/base.py
from abc import ABC, abstractmethod


class BaseTool(ABC):
    name: str
    description: str
    input_schema: dict  # JSON Schema

    @abstractmethod
    async def execute(self, inputs: dict, tenant_context: dict) -> dict: ...

    def as_openai_tool(self) -> dict:
        """Format compatible with model.bind_tools() for all LangChain providers."""
        return {
            "type": "function",
            "function": {
                "name": self.name,
                "description": self.description,
                "parameters": self.input_schema,
            },
        }

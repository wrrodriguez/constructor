# src/runner/agent_runner.py
import asyncio
from langchain_core.messages import AIMessage, HumanMessage
from langchain_core.runnables import RunnableConfig
from langgraph.graph import StateGraph, START, END
from src.runner.state import AgentState
from src.runner.nodes import call_model_node, execute_tool_node, should_continue
from src.router.model_router import ModelRouter
from src.tools.registry import ToolRegistry
from src.events import AgentTask, AgentResult


class AgentRunner:
    def __init__(self, model_router: ModelRouter, tool_registry: ToolRegistry):
        self.model_router = model_router
        self.tool_registry = tool_registry
        self._graph = self._build_graph()

    def _build_graph(self):
        g = StateGraph(AgentState)
        g.add_node("call_model", call_model_node)
        g.add_node("execute_tool", execute_tool_node)
        g.add_edge(START, "call_model")
        g.add_conditional_edges(
            "call_model",
            should_continue,
            {"execute_tool": "execute_tool", END: END},
        )
        g.add_edge("execute_tool", "call_model")
        return g.compile()

    async def run(self, task: AgentTask) -> AgentResult:
        model = self.model_router.get_model(task.task_type, task.model_override)
        tools = [t for name in task.tools_allowed if (t := self.tool_registry.get(name))]

        initial: AgentState = {
            "messages": [HumanMessage(
                content=f"<context>{task.context}</context>\n\n{task.prompt}"
            )],
            "iterations": 0,
            "max_iterations": task.max_iterations,
            "context": task.context,
            "tools_allowed": task.tools_allowed,
            "task_id": str(task.task_id),
            "tenant_id": str(task.tenant_id),
        }
        cfg = RunnableConfig(configurable={
            "model": model,
            "tools": tools,
            "registry": self.tool_registry,
        })

        try:
            final = await asyncio.wait_for(
                self._graph.ainvoke(initial, cfg),
                timeout=task.timeout_seconds,
            )
        except asyncio.TimeoutError:
            return AgentResult(
                task_id=task.task_id, execution_id=task.execution_id,
                tenant_id=task.tenant_id, step_id=task.step_id,
                status="timeout", output={}, tokens_used=0,
                model_used=self.model_router.last_model_used,
                iterations=0, error=f"Timed out after {task.timeout_seconds}s",
            )
        except Exception as exc:
            return AgentResult(
                task_id=task.task_id, execution_id=task.execution_id,
                tenant_id=task.tenant_id, step_id=task.step_id,
                status="failed", output={}, tokens_used=0,
                model_used=self.model_router.last_model_used,
                iterations=0, error=str(exc),
            )

        last_ai = next(
            (m for m in reversed(final["messages"]) if isinstance(m, AIMessage)), None
        )
        # Detect max_iterations hit: last message has tool_calls but loop exited
        if last_ai and last_ai.tool_calls and final["iterations"] >= task.max_iterations:
            return AgentResult(
                task_id=task.task_id, execution_id=task.execution_id,
                tenant_id=task.tenant_id, step_id=task.step_id,
                status="failed", output={}, tokens_used=self._count_tokens(final["messages"]),
                model_used=self.model_router.last_model_used,
                iterations=final["iterations"], error="max_iterations_reached",
            )

        return AgentResult(
            task_id=task.task_id, execution_id=task.execution_id,
            tenant_id=task.tenant_id, step_id=task.step_id,
            status="completed",
            output={"text": last_ai.content if last_ai else ""},
            tokens_used=self._count_tokens(final["messages"]),
            model_used=self.model_router.last_model_used,
            iterations=final["iterations"],
            error=None,
        )

    def _count_tokens(self, messages: list) -> int:
        total = 0
        for m in messages:
            if hasattr(m, "usage_metadata") and m.usage_metadata:
                total += m.usage_metadata.get("total_tokens", 0)
        return total

# src/tools/sql_query.py
import re
import sqlparse
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from src.tools.base import BaseTool
from src.config import settings

_DML_PATTERN = re.compile(
    r"\b(INSERT|UPDATE|DELETE|DROP|CREATE|ALTER|TRUNCATE|GRANT|REVOKE|EXEC|EXECUTE)\b",
    re.IGNORECASE,
)


def _is_select_only(query: str) -> bool:
    statements = sqlparse.parse(query.strip())
    if not statements:
        return False
    for stmt in statements:
        stmt_type = stmt.get_type()
        if stmt_type not in ("SELECT", None):
            return False
        if _DML_PATTERN.search(str(stmt)):
            return False
    return True


class SqlQueryTool(BaseTool):
    name = "sql_query"
    description = "Execute a read-only SQL SELECT query on the tenant database"
    input_schema = {
        "type": "object",
        "properties": {
            "query":  {"type": "string", "description": "SQL SELECT query"},
            "params": {"type": "object", "description": "Named query parameters"},
        },
        "required": ["query"],
    }

    async def execute(self, inputs: dict, tenant_context: dict) -> dict:
        query = inputs["query"]
        params = inputs.get("params", {})

        if not _is_select_only(query):
            raise ValueError("Only SELECT queries are allowed. DML and DDL are rejected.")

        engine = create_async_engine(settings.database_url)
        try:
            factory = async_sessionmaker(engine, expire_on_commit=False)
            async with factory() as session:
                await session.execute(
                    text("SET app.current_tenant = :tid"),
                    {"tid": str(tenant_context.get("tenant_id", ""))},
                )
                result = await session.execute(text(query), params)
                rows = [dict(row._mapping) for row in result.all()]
        finally:
            await engine.dispose()

        return {"rows": rows, "row_count": len(rows)}

# src/tools/http_generic.py
import ipaddress
import socket
import httpx
from src.tools.base import BaseTool

_PRIVATE_NETWORKS = [
    ipaddress.ip_network("10.0.0.0/8"),
    ipaddress.ip_network("172.16.0.0/12"),
    ipaddress.ip_network("192.168.0.0/16"),
    ipaddress.ip_network("127.0.0.0/8"),
    ipaddress.ip_network("169.254.0.0/16"),
    ipaddress.ip_network("::1/128"),
    ipaddress.ip_network("fc00::/7"),
]


def _is_private(host: str) -> bool:
    try:
        ip = ipaddress.ip_address(host)
        return any(ip in net for net in _PRIVATE_NETWORKS)
    except ValueError:
        try:
            resolved = socket.gethostbyname(host)
            ip = ipaddress.ip_address(resolved)
            return any(ip in net for net in _PRIVATE_NETWORKS)
        except (socket.gaierror, ValueError):
            return False


class HttpGenericTool(BaseTool):
    name = "http_generic"
    description = "Make an HTTP GET or POST request to an external URL"
    input_schema = {
        "type": "object",
        "properties": {
            "url":     {"type": "string", "description": "Full URL to request"},
            "method":  {"type": "string", "enum": ["GET", "POST"]},
            "headers": {"type": "object", "description": "HTTP headers"},
            "body":    {"type": "object", "description": "Request body (POST only)"},
        },
        "required": ["url", "method", "headers"],
    }

    async def execute(self, inputs: dict, tenant_context: dict) -> dict:
        from urllib.parse import urlparse
        parsed = urlparse(inputs["url"])
        host = parsed.hostname or ""
        if _is_private(host):
            raise ValueError(f"Access to private/internal networks is not allowed: {host}")

        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.request(
                method=inputs["method"],
                url=inputs["url"],
                headers=inputs.get("headers", {}),
                json=inputs.get("body"),
            )
        try:
            body = response.json()
        except Exception:
            body = response.text

        return {
            "status_code": response.status_code,
            "body": body,
            "headers": dict(response.headers),
        }

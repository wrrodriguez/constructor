# src/auth.py
from typing import Any
import jwt
from jwt import ExpiredSignatureError, InvalidTokenError
from src.config import settings

ALGORITHM = "RS256"


def verify_token(token: str) -> dict[str, Any]:
    """Verifica JWT con clave pública RSA. Lanza ValueError si el token es inválido."""
    try:
        return jwt.decode(
            token,
            settings.jwt_public_key,
            algorithms=[ALGORITHM],
            options={"require": ["exp", "sub", "tenant_id"]},
        )
    except ExpiredSignatureError:
        raise ValueError("Token expired")
    except InvalidTokenError as e:
        raise ValueError(f"Invalid token: {e}")

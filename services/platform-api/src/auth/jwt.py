# src/auth/jwt.py
from datetime import datetime, timedelta, timezone
from typing import Any

import jwt
from jwt import ExpiredSignatureError, InvalidTokenError

from src.config import settings

ALGORITHM = "RS256"


def create_access_token(data: dict[str, Any], expires_delta: timedelta) -> str:
    to_encode = data.copy()
    expire = datetime.now(timezone.utc) + expires_delta
    to_encode["exp"] = expire
    return jwt.encode(to_encode, settings.jwt_private_key, algorithm=ALGORITHM)


def decode_access_token(token: str) -> dict[str, Any]:
    try:
        return jwt.decode(
            token,
            settings.jwt_public_key,
            algorithms=[ALGORITHM],
            options={"require": ["exp", "sub"]},
        )
    except ExpiredSignatureError:
        raise ValueError("Token expired")
    except InvalidTokenError as e:
        raise ValueError(f"Invalid token: {e}")

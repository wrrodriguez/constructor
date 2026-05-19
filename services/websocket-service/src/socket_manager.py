# src/socket_manager.py
import socketio
from src.config import settings

sio = socketio.AsyncServer(async_mode="asgi", cors_allowed_origins=settings.cors_allowed_origins)

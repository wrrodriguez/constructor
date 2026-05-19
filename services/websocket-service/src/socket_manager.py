# src/socket_manager.py
import socketio

sio = socketio.AsyncServer(async_mode="asgi", cors_allowed_origins="*")

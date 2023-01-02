import websocket
from typing import Any, Dict

class EnhancedWebsocket:
    socket: websocket.WebSocketApp
    token: str
    def __init__(self, socket: websocket.WebSocketApp, *, token: str = ...) -> None: ...
    @property
    def is_private(self) -> bool: ...
    def send_json(self, payload: Dict[str, Any]): ...

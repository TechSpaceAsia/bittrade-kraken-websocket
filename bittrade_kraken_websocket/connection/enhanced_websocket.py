from logging import getLogger
from threading import Lock
from typing import Any, Optional, Dict

import orjson
import websocket
from reactivex import Observable

logger = getLogger(__name__)


class EnhancedWebsocket():
    socket: websocket.WebSocketApp
    token: str = ''

    def __init__(self, socket: websocket.WebSocketApp, *, token=''):
        self.socket = socket
        self.token = token

    @property
    def is_private(self) -> bool:
        return bool(self.token)

    def send_json(self, payload: Dict[str, Any]):
        if self.is_private:
            # if subscription, token goes into that, otherwise goes to top level
            put_token_into = payload.get('subscription', payload)
            put_token_into['token'] = self.token
        as_bytes = orjson.dumps(payload)
        logger.debug('[SOCKET] Sending json to socket: %s', as_bytes)
        return self.socket.send(as_bytes)


__all__ = [
    "EnhancedWebsocket"
]
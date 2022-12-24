from logging import getLogger
from threading import Lock
from typing import Optional, Dict

import orjson
import websocket
from reactivex import Observable

logger = getLogger(__name__)


class EnhancedWebsocket():
    socket: websocket.WebSocketApp
    token: str = ''
    _token_generator: Observable[str]
    _lock: Lock

    def __init__(self, socket: websocket.WebSocketApp, *, token_generator: Optional[Observable[str]] = None, token=''):
        # Note that the token_generator will not be used if token is passed
        self.socket = socket
        self._token_generator = token_generator
        self._lock = Lock()
        self.token = token

    @property
    def is_private(self) -> bool:
        return self._token_generator is not None or bool(self.token)

    def send_json(self, payload: Dict):
        # private socket always requires token
        if self.is_private and not self.token:
            with self._lock:
                self.token = self._token_generator.run()
        if self.is_private:
            # if subscription, token goes into that, otherwise goes to top level
            put_token_into = payload.get('subscription', payload)
            put_token_into['token'] = self.token
        as_bytes = orjson.dumps(payload)
        logger.debug('[SOCKET] Sending json to socket: %s', as_bytes)
        return self.socket.send(as_bytes)

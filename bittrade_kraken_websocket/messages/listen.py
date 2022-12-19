from typing import List, Dict, Callable

import orjson
from reactivex import operators, Observable, compose

from bittrade_kraken_websocket.connection.generic import WebsocketBundle, WEBSOCKET_MESSAGE

def _is_message(message: WebsocketBundle):
    return message[1] == WEBSOCKET_MESSAGE

def _message_only(json_messages: bool):
    def fn(message: WebsocketBundle):
        m = message[2]
        if json_messages:
            return orjson.loads(m)
        return m

    return fn

def keep_messages_only(json_messages=False) -> Callable[[Observable[WebsocketBundle]], Observable[str | Dict | List]]:
    return compose(
        operators.filter(_is_message),
        operators.map(_message_only(json_messages)),
    )

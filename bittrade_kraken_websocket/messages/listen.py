from typing import List, Dict, Callable

import orjson
from reactivex import operators, Observable, compose

from bittrade_kraken_websocket.connection.connection_operators import filter_socket_status_only
from bittrade_kraken_websocket.connection.enhanced_websocket import EnhancedWebsocket
from bittrade_kraken_websocket.connection.generic import WebsocketBundle, WEBSOCKET_MESSAGE, WEBSOCKET_STATUS
from bittrade_kraken_websocket.connection.status import Status


def _is_message(message: WebsocketBundle):
    return message[1] == WEBSOCKET_MESSAGE


def message_only() -> Callable[[Observable[WebsocketBundle]], Observable[Status | Dict | List]]:
    return operators.map(lambda x: x[2])


def keep_messages_only() -> Callable[[Observable[WebsocketBundle]], Observable[Dict | List]]:
    return compose(
        operators.filter(_is_message),
        message_only(),
    )


def keep_status_only() -> Callable[[Observable], Observable[Status]]:
    return compose(
        filter_socket_status_only(),
        message_only()
    )


def filter_new_socket_only() -> Callable[[Observable[WebsocketBundle]], Observable[EnhancedWebsocket]]:
    return compose(
        operators.map(lambda x: x[0]),
        operators.distinct_until_changed(),
    )
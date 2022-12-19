from typing import Callable

from reactivex import compose, operators, Observable

from bittrade_kraken_websocket.connection.generic import WebsocketBundle, EnhancedWebsocket, WEBSOCKET_STATUS
from bittrade_kraken_websocket.connection.status import WEBSOCKET_OPENED


def socket_status_only() -> Callable[[Observable[WebsocketBundle]], Observable[WebsocketBundle]]:
    """Grab only messages related to the status of the websocket connection"""
    return operators.filter(lambda x: x[1] == WEBSOCKET_STATUS)


def socket_only() -> Callable[[Observable[WebsocketBundle]], Observable[EnhancedWebsocket]]:
    """Returns an observable that represents the websocket only whenever emitted"""
    return operators.map(lambda x: x[0])


def connected_socket() -> Callable[[Observable[WebsocketBundle]], Observable[EnhancedWebsocket]]:
    """
    Observable emits connected sockets only - useful for authentication
    """
    return compose(
        socket_status_only(),
        operators.filter(lambda x: x[2] == WEBSOCKET_OPENED),
        socket_only()
    )

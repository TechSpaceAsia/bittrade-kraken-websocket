from typing import Callable, List, Optional, Tuple

from reactivex import compose, operators, Observable

from bittrade_kraken_websocket.connection.generic import WebsocketBundle, EnhancedWebsocket, WEBSOCKET_STATUS
from bittrade_kraken_websocket.connection.status import WEBSOCKET_OPENED, WEBSOCKET_AUTHENTICATED, Status, \
    WEBSOCKET_SYSTEM_ONLINE, WEBSOCKET_CLOSED


ReadyMessage = Tuple[EnhancedWebsocket, bool]


def filter_socket_status_only() -> Callable[[Observable[WebsocketBundle]], Observable[WebsocketBundle]]:
    def is_status(x):
        return x[1] == WEBSOCKET_STATUS

    """Grab only messages related to the status of the websocket connection"""
    return operators.filter(is_status)


def map_socket_only() -> Callable[[Observable[WebsocketBundle | ReadyMessage]], Observable[EnhancedWebsocket]]:
    """Returns an observable that represents the websocket only whenever emitted"""
    return operators.map(lambda x: x[0])


def connected_socket() -> Callable[[Observable[WebsocketBundle]], Observable[EnhancedWebsocket]]:
    return compose(
        ready_socket([WEBSOCKET_OPENED]),
        operators.filter(lambda m: m[1]),
        map_socket_only(),
    )


def ready_socket(up_statuses: Optional[List[Status]] = None, down_statuses: Optional[List[Status]] = None) -> Callable[
    [Observable[WebsocketBundle]], Observable[ReadyMessage]]:
    up_statuses = up_statuses or [WEBSOCKET_SYSTEM_ONLINE]
    down_statuses = down_statuses or [WEBSOCKET_CLOSED]
    """
    Observable emits connected sockets only - useful for authentication
    """
    return compose(
        filter_socket_status_only(),
        operators.filter(lambda x: x[2] in up_statuses or x[2] in down_statuses),
        operators.map(lambda x: (x[0], x[2] in up_statuses,)),  # becomes (socket, true/false <- readiness)
        operators.distinct_until_changed(lambda x: x[1])  # No need to publish multiple times that we are ready/down
    )


def authenticated_socket() -> Callable[[Observable[WebsocketBundle]], Observable[EnhancedWebsocket]]:
    """
    Observable emits authenticated sockets only - useful for private subscriptions
    """
    return compose(
        filter_socket_status_only(),
        operators.filter(lambda x: x[2] == WEBSOCKET_AUTHENTICATED),
        map_socket_only()
    )

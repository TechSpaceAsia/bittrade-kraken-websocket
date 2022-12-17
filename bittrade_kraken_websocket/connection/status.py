from typing import Literal

from reactivex import compose, operators, Observable
from websocket import WebSocketApp

WEBSOCKET_OPENED = "WEBSOCKET_OPENED"
WEBSOCKET_CLOSED = "WEBSOCKET_CLOSED"
Status = Literal[WEBSOCKET_OPENED, WEBSOCKET_CLOSED]


def connected_socket() -> Observable[WebSocketApp]:
    return compose(
        operators.filter(lambda x: x[1] == WEBSOCKET_OPENED),
        operators.map(lambda x: x[0])
    )

from typing import overload, Literal

from reactivex import Observable, ConnectableObservable
from reactivex.operators import publish

from .reconnect import retry_with_backoff
from bittrade_kraken_websocket.connection.generic import websocket_connection, WebsocketBundle

@overload
def public_websocket_connection() -> ConnectableObservable[WebsocketBundle]:
    ...

@overload
def public_websocket_connection(reconnect: bool) -> ConnectableObservable[WebsocketBundle]:
    ...


@overload
def public_websocket_connection(reconnect: bool, shared: Literal[True]) -> ConnectableObservable[WebsocketBundle]:
    ...


@overload
def public_websocket_connection(reconnect: bool, shared: Literal[False]) -> Observable[WebsocketBundle]:
    ...


def public_websocket_connection(reconnect: bool = False, shared: bool = True) -> ConnectableObservable[
                                                                                     WebsocketBundle] | Observable[
                                                                                     WebsocketBundle]:
    ops = []
    if reconnect:
        ops.append(retry_with_backoff())
    if shared:
        ops.append(publish())
    return websocket_connection().pipe(
        *ops
    )

from logging import getLogger
from typing import overload, Literal

from reactivex import Observable, ConnectableObservable
from reactivex.operators import publish

from bittrade_kraken_websocket.connection.generic import websocket_connection, WebsocketBundle
from bittrade_kraken_websocket.connection.reconnect import retry_with_backoff

logger = getLogger(__name__)


@overload
def private_websocket_connection(token_generator: Observable[str]) -> ConnectableObservable[WebsocketBundle]:
    ...


@overload
def private_websocket_connection(
        token_generator: Observable[str], reconnect: bool
) -> ConnectableObservable[WebsocketBundle]:
    ...


@overload
def private_websocket_connection(
        token_generator: Observable[str], reconnect: bool, shared: Literal[True]
) -> ConnectableObservable[WebsocketBundle]:
    ...


@overload
def private_websocket_connection(
        token_generator: Observable[str], reconnect: bool, shared: Literal[False]
) -> Observable[WebsocketBundle]:
    ...


def private_websocket_connection(token_generator: Observable[str], reconnect: bool = False, shared: bool = True):
    """Token generator is an observable which is expected to emit a single item upon subscription then complete.
    An example implementation can be found in `examples/private_subscription.py`"""
    ops = []
    if reconnect:
        ops.append(retry_with_backoff())
    if shared:
        ops.append(publish())
    
    return websocket_connection(
        token_generator
    ).pipe(
        *ops
    )

from logging import getLogger
from typing import Optional, overload, Literal

from reactivex import Observable, ConnectableObservable
from reactivex.abc import SchedulerBase
from reactivex.operators import publish

from bittrade_kraken_websocket.connection.generic import websocket_connection, WebsocketBundle
from bittrade_kraken_websocket.connection.reconnect import retry_with_backoff

logger = getLogger(__name__)


@overload
def private_websocket_connection() -> ConnectableObservable[WebsocketBundle]:
    ...

@overload
def private_websocket_connection(*, scheduler: Optional[SchedulerBase]) -> ConnectableObservable[WebsocketBundle]:
    ...

@overload
def private_websocket_connection(*, reconnect: bool) -> ConnectableObservable[WebsocketBundle]:
    ...


@overload
def private_websocket_connection(
        *, reconnect: bool, scheduler: Optional[SchedulerBase]
) -> ConnectableObservable[WebsocketBundle]:
    ...


@overload
def private_websocket_connection(
        *, reconnect: bool, shared: Literal[True], scheduler: Optional[SchedulerBase]
) -> ConnectableObservable[WebsocketBundle]:
    ...


@overload
def private_websocket_connection(*,
        reconnect: bool, shared: Literal[False], scheduler: Optional[SchedulerBase]
) -> Observable[WebsocketBundle]:
    ...


def private_websocket_connection(*, reconnect: bool = False, shared: bool = True, scheduler: Optional[SchedulerBase] = None):
    """Token generator is an observable which is expected to emit a single item upon subscription then complete.
    An example implementation can be found in `examples/private_subscription.py`"""
    ops = []
    if reconnect:
        ops.append(retry_with_backoff())
    if shared:
        ops.append(publish())
    
    return websocket_connection(
        private=True, scheduler=scheduler
    ).pipe(
        *ops
    )

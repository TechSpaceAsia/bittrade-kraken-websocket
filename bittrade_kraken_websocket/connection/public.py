from typing import Optional, overload, Literal

from reactivex import Observable, ConnectableObservable
from reactivex.operators import publish
from reactivex.abc import SchedulerBase

from .reconnect import retry_with_backoff
from bittrade_kraken_websocket.connection.generic import websocket_connection, WebsocketBundle

@overload
def public_websocket_connection() -> ConnectableObservable[WebsocketBundle]:
    ...


@overload
def public_websocket_connection(*, scheduler: Optional[SchedulerBase]) -> ConnectableObservable[WebsocketBundle]:
    ...

    
@overload
def public_websocket_connection(*, reconnect: bool) -> ConnectableObservable[WebsocketBundle]:
    ...


@overload
def public_websocket_connection(*, reconnect: bool, scheduler: Optional[SchedulerBase]) -> ConnectableObservable[WebsocketBundle]:
    ...


@overload
def public_websocket_connection(*, reconnect: bool, shared: Literal[True], scheduler: Optional[SchedulerBase]) -> ConnectableObservable[WebsocketBundle]:
    ...


@overload
def public_websocket_connection(*, reconnect: bool, shared: Literal[False], scheduler: Optional[SchedulerBase]) -> Observable[WebsocketBundle]:
    ...


def public_websocket_connection(*, reconnect: bool = False, shared: bool = True, scheduler: Optional[SchedulerBase] = None) -> ConnectableObservable[
                                                                                     WebsocketBundle] | Observable[
                                                                                     WebsocketBundle]:
    ops = []
    if reconnect:
        ops.append(retry_with_backoff())
    if shared:
        ops.append(publish())
    return websocket_connection(scheduler=scheduler).pipe(
        *ops
    )


__all__ = [
    "public_websocket_connection",
]
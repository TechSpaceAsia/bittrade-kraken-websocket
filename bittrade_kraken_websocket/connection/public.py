from typing import Optional

from reactivex import ConnectableObservable
from reactivex.operators import publish
from reactivex.abc import SchedulerBase

from .reconnect import retry_with_backoff
from bittrade_kraken_websocket.connection.generic import websocket_connection, WebsocketBundle


def public_websocket_connection(*, reconnect: bool = False, scheduler: Optional[SchedulerBase] = None) -> ConnectableObservable[
                                                                                     WebsocketBundle]:
    ops = []
    if reconnect:
        ops.append(retry_with_backoff())
    ops.append(publish())
    return websocket_connection(scheduler=scheduler).pipe(
        *ops
    )

__all__ = [
    "public_websocket_connection",
]
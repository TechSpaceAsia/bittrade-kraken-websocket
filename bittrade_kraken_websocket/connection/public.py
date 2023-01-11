from typing import Optional

from reactivex import ConnectableObservable
from reactivex.operators import publish
from reactivex.abc import SchedulerBase

from .reconnect import retry_with_backoff
from bittrade_kraken_websocket.connection.generic import websocket_connection, WebsocketBundle


def public_websocket_connection(*, reconnect: bool = True, scheduler: Optional[SchedulerBase] = None) -> ConnectableObservable[
                                                                                     WebsocketBundle]:
    connection = websocket_connection(scheduler=scheduler)
    if reconnect:
        connection = connection.pipe(retry_with_backoff())
    return connection.pipe(publish())
    
__all__ = [
    "public_websocket_connection",
]
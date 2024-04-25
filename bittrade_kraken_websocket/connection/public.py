from typing import Optional

from reactivex import ConnectableObservable
from reactivex.operators import publish
from reactivex.abc import SchedulerBase

from .reconnect import retry_with_backoff

from elm_framework_helpers.websockets.models import WebsocketBundle
from bittrade_kraken_websocket.connection.generic import websocket_connection


def public_websocket_connection(*, reconnect: bool = True, scheduler: Optional[SchedulerBase] = None) -> ConnectableObservable[
                                                                                     WebsocketBundle]:
    connection = websocket_connection(scheduler=scheduler)
    if reconnect:
        connection = connection.pipe(retry_with_backoff())
    return connection.pipe(publish())
    
__all__ = [
    "public_websocket_connection",
]
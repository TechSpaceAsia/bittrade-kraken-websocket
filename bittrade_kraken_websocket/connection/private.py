from logging import getLogger
from typing import Optional

from reactivex import ConnectableObservable
from reactivex.abc import SchedulerBase
from reactivex.operators import publish

from bittrade_kraken_websocket.connection.generic import websocket_connection, WebsocketBundle
from bittrade_kraken_websocket.connection.reconnect import retry_with_backoff

logger = getLogger(__name__)


def private_websocket_connection(*, reconnect: bool = True, scheduler: Optional[SchedulerBase] = None) -> ConnectableObservable[WebsocketBundle]:
    """You need to add your token to the EnhancedWebsocket
    An example implementation can be found in `examples/private_subscription.py`"""
    connection = websocket_connection(
        private=True, scheduler=scheduler
    )
    if reconnect:
        connection = connection.pipe(retry_with_backoff())
    
    return connection.pipe(
        publish()
    )

__all__ = [
    "private_websocket_connection",
]

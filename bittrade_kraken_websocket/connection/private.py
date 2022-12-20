from logging import getLogger

from reactivex import Observable

from bittrade_kraken_websocket.connection.generic import websocket_connection

logger = getLogger(__name__)


def private_websocket_connection(token_generator: Observable[str]):
    """Token generator is an observable which is expected to emit a single item upon subscription then complete.
    An example implementation can be found in `examples/private_subscription.py`"""
    return websocket_connection(
        token_generator
    )

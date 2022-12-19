from typing import Dict

from reactivex import Observable, Observer
from reactivex.operators import take

from bittrade_kraken_websocket.connection.generic import websocket_connection, WebsocketBundle, WEBSOCKET_STATUS
from bittrade_kraken_websocket.connection.status import WEBSOCKET_AUTHENTICATED


def add_token(token_generator: Observable[str]):
    def _add_token(source: Observable[WebsocketBundle]) -> Observable[WebsocketBundle]:

        def subscribe(observer: Observer, scheduler=None):
            def on_next(bundle: WebsocketBundle):
                connection, *_ = bundle
                try:
                    token = token_generator.pipe(take(1)).run()
                    connection.token = token
                    observer.on_next([connection, WEBSOCKET_STATUS, WEBSOCKET_AUTHENTICATED])
                except Exception as exc:
                    observer.on_error(exc)
            return source.subscribe(
                on_next=on_next,
                on_completed=observer.on_completed, on_error=observer.on_error, scheduler=scheduler
            )
        return Observable(subscribe)
    return _add_token


def private_websocket_connection(token_generator: Observable[str], json_messages=False):
    """Token generator is an observable which is expected to emit a single item upon subscription. An implementation can be seen in `examples/private_subscription.py`"""
    return websocket_connection(
        'wss://ws-auth.kraken.com', json_messages=json_messages
    ).pipe(
        add_token(token_generator)
    )

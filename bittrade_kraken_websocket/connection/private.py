from typing import Dict

from reactivex import Observable, Observer
from reactivex.operators import flat_map, take

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
            source.subscribe(
                on_next=on_next,
                on_completed=observer.on_completed, on_error=observer.on_error, scheduler=scheduler
            )
        return Observable(subscribe)
    return _add_token


def private_websocket_connection(token_generator: Observable[str]):
    return websocket_connection(
        'wss://ws-auth.kraken.com'
    ).pipe(
        add_token(token_generator)
    )

from reactivex import Observable, ConnectableObservable
from reactivex.operators import publish

from bittrade_kraken_websocket.connection.generic import websocket_connection, WebsocketMessage


def public_websocket_connection() -> Observable[WebsocketMessage]:
    return websocket_connection(
        'wss://ws.kraken.com'
    )


def public_websocket_connection_multicast() -> ConnectableObservable[WebsocketMessage]:
    return public_websocket_connection().pipe(
        publish()
    )

{"event":"subscribe","pair":["USDT/USD"],"subscription":{"name":"ticker"}}
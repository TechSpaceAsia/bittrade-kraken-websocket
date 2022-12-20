from reactivex import Observable, ConnectableObservable
from reactivex.operators import publish

from bittrade_kraken_websocket.connection.generic import websocket_connection, WebsocketBundle


def public_websocket_connection() -> Observable[WebsocketBundle]:
    return websocket_connection()

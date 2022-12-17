from concurrent.futures import ThreadPoolExecutor
from typing import TypeVar, Tuple
from logging import getLogger

import reactivex.disposable
from reactivex import Observable, Observer
import websocket
from bittrade_kraken_websocket.connection.status import WEBSOCKET_OPENED, WEBSOCKET_CLOSED, Status

logger = getLogger(__name__)

WebsocketMessage = Tuple[websocket.WebSocketApp, Status]
def websocket_connection(url: str) -> Observable[WebsocketMessage]:
    def subscribe(observer: Observer, scheduler=None):
        def on_error(ws, error):
            logger.error('Websocket errored %s', error)
            observer.on_error(error)

        def on_close(ws, close_status_code, close_msg):
            logger.warning('Websocket closed %s %s', close_status_code, close_msg)
            observer.on_next((ws, WEBSOCKET_CLOSED))
            observer.on_completed()

        def on_open(ws):
            logger.info('Websocket opened')
            observer.on_next((ws, WEBSOCKET_OPENED))

        connection = websocket.WebSocketApp(
            url, on_open=on_open, on_close=on_close, on_error=on_error
        )
        executor = ThreadPoolExecutor(thread_name_prefix='WebsocketPool')
        executor.submit(connection.run_forever)
        executor.shutdown(wait=False)
        def disconnect():
            logger.info('Releasing resources')
            connection.close()
        return reactivex.disposable.Disposable(disconnect)
    return Observable(subscribe)

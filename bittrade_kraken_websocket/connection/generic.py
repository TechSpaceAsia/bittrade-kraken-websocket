from concurrent.futures import ThreadPoolExecutor
from threading import Lock
from typing import TypeVar, Tuple, Dict, Literal, Union, List, Optional
from logging import getLogger

import orjson
import reactivex.disposable
from reactivex import Observable, Observer
import websocket
from websocket import WebSocketConnectionClosedException, ABNF

from bittrade_kraken_websocket.connection.enhanced_websocket import EnhancedWebsocket
from bittrade_kraken_websocket.connection.status import WEBSOCKET_OPENED, WEBSOCKET_CLOSED, Status
from bittrade_kraken_websocket.messages.heartbeat import HEARTBEAT

logger = getLogger(__name__)


WEBSOCKET_STATUS = 'WEBSOCKET_STATUS'
WEBSOCKET_HEARTBEAT = 'WEBSOCKET_HEARTBEAT'
WEBSOCKET_MESSAGE = 'WEBSOCKET_MESSAGE'
MessageTypes = Literal[WEBSOCKET_STATUS, WEBSOCKET_HEARTBEAT, WEBSOCKET_MESSAGE]

WebsocketBundle = Tuple[EnhancedWebsocket, MessageTypes, Union[Status, Dict, List]]


def websocket_connection(token_generator: Optional[Observable[str]] = None) -> Observable[WebsocketBundle]:
    is_private = token_generator is not None
    url = f'wss://ws{"-auth" if is_private else ""}.kraken.com'

    def subscribe(observer: Observer, scheduler=None):
        def on_error(ws, error):
            logger.error('[SOCKET] Websocket errored %s', error)
            observer.on_next((enhanced, WEBSOCKET_STATUS, WEBSOCKET_CLOSED))
            observer.on_error(error)

        def on_close(ws, close_status_code, close_msg):
            logger.warning('[SOCKET] Websocket closed | status: %s, close message: %s', close_status_code, close_msg)
            observer.on_next((enhanced, WEBSOCKET_STATUS, WEBSOCKET_CLOSED))
            observer.on_error(Exception('Socket closed'))

        def on_open(ws):
            logger.info('[SOCKET] Websocket opened')
            observer.on_next((enhanced, WEBSOCKET_STATUS, WEBSOCKET_OPENED))

        def on_message(ws, message):
            pass_message = orjson.loads(message)
            category = WEBSOCKET_MESSAGE
            if message == HEARTBEAT:
                category = WEBSOCKET_HEARTBEAT
            else:
                logger.debug('[SOCKET] %s', message)
                if type(pass_message) == dict and pass_message.get('event') == 'systemStatus':
                    category = WEBSOCKET_STATUS
                    pass_message = pass_message['status']
            observer.on_next((enhanced, category, pass_message))

        connection = websocket.WebSocketApp(
            url, on_open=on_open, on_close=on_close, on_error=on_error, on_message=on_message
        )
        enhanced = EnhancedWebsocket(connection, token_generator)
        executor = ThreadPoolExecutor(thread_name_prefix='WebsocketPool')
        executor.submit(connection.run_forever)
        executor.shutdown(wait=False)

        def disconnect():
            logger.info('[SOCKET] Releasing resources')
            try:
                connection.close()
            except WebSocketConnectionClosedException as exc:
                logger.error('[SOCKET] Socket was already closed %s', exc)

        return reactivex.disposable.Disposable(disconnect)

    return Observable(subscribe)

from concurrent.futures import ThreadPoolExecutor
from typing import TypeVar, Tuple, Dict, Literal, Union, List
from logging import getLogger

import orjson
import reactivex.disposable
from reactivex import Observable, Observer
import websocket
from websocket import WebSocketConnectionClosedException

from bittrade_kraken_websocket.connection.status import WEBSOCKET_OPENED, WEBSOCKET_CLOSED, Status
from bittrade_kraken_websocket.messages.heartbeat import HEARTBEAT

logger = getLogger(__name__)


class EnhancedWebsocket():
    socket: websocket.WebSocketApp
    token: ''

    def __init__(self, socket, token=''):
        self.socket = socket
        self.token = token

    def send_json(self, payload: Dict):
        # private socket always requires token
        if self.token:
            # if subscription, token goes into that, otherwise goes to top level
            put_token_into = payload.get('subscription', payload)
            put_token_into['token'] = self.token
        return self.socket.send(orjson.dumps(payload))


WEBSOCKET_STATUS = 'WEBSOCKET_STATUS'
WEBSOCKET_HEARTBEAT = 'WEBSOCKET_HEARTBEAT'
WEBSOCKET_MESSAGE = 'WEBSOCKET_MESSAGE'
MessageTypes = Literal[WEBSOCKET_STATUS, WEBSOCKET_HEARTBEAT, WEBSOCKET_MESSAGE]

WebsocketBundle = Tuple[EnhancedWebsocket, MessageTypes, Union[Status, str, Dict, List]]


def websocket_connection(url: str, json_messages=False) -> Observable[WebsocketBundle]:
    def subscribe(observer: Observer, scheduler=None):
        def on_error(ws, error):
            logger.error('Websocket errored %s', error)
            observer.on_next((enhanced, WEBSOCKET_STATUS, WEBSOCKET_CLOSED))
            observer.on_error(error)

        def on_close(ws, close_status_code, close_msg):
            logger.warning('Websocket closed %s %s', close_status_code, close_msg)
            observer.on_next((enhanced, WEBSOCKET_STATUS, WEBSOCKET_CLOSED))
            observer.on_error(Exception('Socket closed'))

        def on_open(ws):
            logger.info('Websocket opened')
            observer.on_next((enhanced, WEBSOCKET_STATUS, WEBSOCKET_OPENED))

        def on_message(ws, message):
            category = WEBSOCKET_MESSAGE if message != HEARTBEAT else WEBSOCKET_HEARTBEAT
            observer.on_next((enhanced, category, message if not json_messages else orjson.loads(message)))

        connection = websocket.WebSocketApp(
            url, on_open=on_open, on_close=on_close, on_error=on_error, on_message=on_message
        )
        enhanced = EnhancedWebsocket(connection)
        executor = ThreadPoolExecutor(thread_name_prefix='WebsocketPool')
        executor.submit(connection.run_forever)
        executor.shutdown(wait=False)

        def disconnect():
            logger.info('Releasing resources')
            try:
                connection.close()
            except WebSocketConnectionClosedException as exc:
                logger.error('Socket was already closed %s', exc)

        return reactivex.disposable.Disposable(disconnect)

    return Observable(subscribe)

from logging import getLogger
from typing import Any, Tuple, Dict, Literal, Union, List, Optional

import orjson
import reactivex.disposable
from reactivex import Observable
from reactivex.abc import SchedulerBase, ObserverBase
from reactivex.scheduler import ThreadPoolScheduler
from websocket import WebSocketConnectionClosedException, WebSocketApp

from bittrade_kraken_websocket.connection.enhanced_websocket import EnhancedWebsocket
from bittrade_kraken_websocket.connection.status import (
    WEBSOCKET_OPENED,
    WEBSOCKET_CLOSED,
    Status,
)
from ..messages.heartbeat import HEARTBEAT

logger = getLogger(__name__)


WEBSOCKET_STATUS = "WEBSOCKET_STATUS"
WEBSOCKET_HEARTBEAT = "WEBSOCKET_HEARTBEAT"
WEBSOCKET_MESSAGE = "WEBSOCKET_MESSAGE"
MessageTypes = Literal["WEBSOCKET_STATUS", "WEBSOCKET_HEARTBEAT", "WEBSOCKET_MESSAGE"]

WebsocketBundle = Tuple[EnhancedWebsocket, MessageTypes, Union[Status, Dict[str, Any], List[Any]]]


def websocket_connection(private: bool = False, scheduler: Optional[SchedulerBase] = None) -> Observable[WebsocketBundle]:
    url = f'wss://ws{"-auth" if private else ""}.kraken.com'
    return raw_websocket_connection(url, scheduler=scheduler)


def raw_websocket_connection(url: str, scheduler: Optional[SchedulerBase] = None) -> Observable[WebsocketBundle]:
    def subscribe(observer: ObserverBase[WebsocketBundle], scheduler_: Optional[SchedulerBase] = None):
        _scheduler = scheduler or scheduler_ or ThreadPoolScheduler()
        connection: WebSocketApp | None = None
        def action(*args: Any):
            nonlocal connection
            def on_error(_ws: WebSocketApp, error: Exception):
                logger.error("[SOCKET][RAW] Websocket errored %s", error)
                observer.on_next((enhanced, WEBSOCKET_STATUS, WEBSOCKET_CLOSED))
                observer.on_error(error)

            def on_close(_ws: WebSocketApp, close_status_code: int, close_msg: str):
                logger.warning(
                    "[SOCKET][RAW] Websocket closed | status: %s, close message: %s",
                    close_status_code,
                    close_msg,
                )
                observer.on_next((enhanced, WEBSOCKET_STATUS, WEBSOCKET_CLOSED))
                observer.on_error(Exception("Socket closed"))

            def on_open(_ws: WebSocketApp):
                logger.info("[SOCKET][RAW] Websocket opened")
                observer.on_next((enhanced, WEBSOCKET_STATUS, WEBSOCKET_OPENED))

            def on_message(_ws: WebSocketApp, message: bytes | str):
                pass_message = orjson.loads(message)
                category = WEBSOCKET_MESSAGE
                if message == HEARTBEAT:
                    category = WEBSOCKET_HEARTBEAT
                else:
                    logger.debug("[SOCKET][RAW] %s", message)
                    if (
                        type(pass_message) == dict
                        and pass_message.get("event") == "systemStatus"
                    ):
                        category = WEBSOCKET_STATUS
                        pass_message = pass_message["status"]
                try:
                    observer.on_next((enhanced, category, pass_message))
                except:
                    logger.exception("[SOCKET] Error on socket message")

            connection = WebSocketApp(
                url,
                on_open=on_open,
                on_close=on_close,
                on_error=on_error,
                on_message=on_message,
            )
            enhanced = EnhancedWebsocket(connection)
            def run_forever(*args: Any):
                assert connection is not None
                connection.run_forever()
            _scheduler.schedule(run_forever)

        def disconnect():
            logger.info("[SOCKET] Releasing resources")
            assert connection is not None
            try:
                connection.close()
            except WebSocketConnectionClosedException as exc:
                logger.error("[SOCKET] Socket was already closed %s", exc)
        
        return reactivex.disposable.CompositeDisposable(
            _scheduler.schedule(action),
            reactivex.disposable.Disposable(disconnect)
        )

    return Observable(subscribe)


__all__ = ["websocket_connection"]

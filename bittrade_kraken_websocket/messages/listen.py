from reactivex import operators, Observer, Observable

from bittrade_kraken_websocket.connection.generic import WebsocketMessage
from bittrade_kraken_websocket.connection.status import WEBSOCKET_OPENED
from bittrade_kraken_websocket.messages.heartbeat import HEARTBEAT


def get_messages():
    def _get_messages(source: Observable[WebsocketMessage]) -> Observable[str]:
        def subscribe(observer: Observer, scheduler=None):
            def on_next(message: WebsocketMessage):
                connection, status = message
                if status == WEBSOCKET_OPENED:
                    connection.on_message = lambda _ws, data: observer.on_next(data)

            return source.subscribe(
                on_next=on_next,
                on_completed=observer.on_completed,
                on_error=observer.on_error,
                scheduler=scheduler
            )

        return Observable(subscribe)
    return _get_messages

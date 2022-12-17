from typing import Tuple, Dict, List
from logging import getLogger

import orjson
from reactivex import Observable, Observer, operators, Subject, interval
from reactivex.disposable import CompositeDisposable
from reactivex.operators import replay, take
from websocket import WebSocketApp

from bittrade_kraken_websocket.channels import CHANNEL_TICKER
from bittrade_kraken_websocket.events.events import EVENT_SUBSCRIBE
from bittrade_kraken_websocket.events.request_response import request_response
from bittrade_kraken_websocket.messages.filters.kind import filter_channel_messages

logger = getLogger(__name__)
def subscribe(channel: str, pair: str=''):
    def _subscribe(source: Observable[Tuple[WebSocketApp, Dict | List]]):
        def observable_subscribe(observer: Observer, scheduler=None):
            pass
        return Observable(observable_subscribe)
    return _subscribe

def subscribe_ticker(messages: Observable[Dict | List], *pairs, timeout=None):
    MESSAGE = {
        "event": EVENT_SUBSCRIBE,
        "pair": pairs,
        "subscription": {
            "name": CHANNEL_TICKER
        }
    }
    timeout = timeout or interval(1.0)
    sender = Subject()
    caller = request_response(sender, messages, timeout, EVENT_SUBSCRIBE, raise_on_status=True)
    def _subscribe_ticker(source: Observable[Observer]) -> Observable[List]:
        def factory(observer: Observer, scheduler=None):
            recorded_messages = messages.pipe(
                filter_channel_messages(CHANNEL_TICKER),
                operators.filter(lambda x: x[3] in pairs),
                replay()
            )
            recorded_messages.subscribe(
                observer
            )
            def on_socket(connection):
                sender_sub = sender.pipe(
                    operators.take(1)
                ).subscribe(on_next=lambda m: connection.send(orjson.dumps(m)), scheduler=scheduler)
                caller(MESSAGE).subscribe(
                    on_next=lambda m: logger.info('Subscription message received'),
                    on_completed=lambda: logger.debug('COMPLETED'),
                    on_error=lambda err: observer.on_error(err) and logger.error('Failed to get subscription message %s', err) and sender_sub.dispose()
                )


            sub = source.subscribe(on_next=on_socket, scheduler=scheduler)
            return CompositeDisposable(
                sub,
                recorded_messages.connect(scheduler=scheduler)
            )
        return Observable(factory)
    return _subscribe_ticker
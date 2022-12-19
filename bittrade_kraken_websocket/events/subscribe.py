from typing import Tuple, Dict, List
from logging import getLogger

import orjson
from reactivex import Observable, Observer, operators, Subject, interval
from reactivex.disposable import CompositeDisposable, Disposable
from reactivex.operators import replay, take, publish
from reactivex.subject import BehaviorSubject
from websocket import WebSocketApp

from bittrade_kraken_websocket.channels import CHANNEL_TICKER
from bittrade_kraken_websocket.connection.generic import EnhancedWebsocket
from bittrade_kraken_websocket.events.events import EVENT_SUBSCRIBE, EVENT_UNSUBSCRIBE
from bittrade_kraken_websocket.events.request_response import request_response
from bittrade_kraken_websocket.messages.filters.kind import filter_channel_messages

logger = getLogger(__name__)
def subscribe_to_channel(messages: Observable[Dict | List], channel: str, pair: str= '', timeout=None):
    timeout = timeout or interval(1.0)
    request_message = {
        "event": EVENT_SUBSCRIBE,
        "subscription": {
            "name": channel
        }
    }
    if pair:
        request_message['pair'] = [pair]

    unsubscribe_request_message = dict(request_message)
    unsubscribe_request_message['event'] = EVENT_UNSUBSCRIBE

    sender = Subject()
    caller = request_response(sender, messages, timeout, EVENT_SUBSCRIBE, raise_on_status=True)
    def _subscribe(source: Observable[EnhancedWebsocket]):

        def on_socket_emitted(connection: EnhancedWebsocket):
            sender.pipe(
                take(1),
            ).subscribe(
                on_next=connection.send_json
            )
            caller(request_message).subscribe()

        def observable_subscribe(observer: Observer, scheduler=None):
            socket_bs: BehaviorSubject = BehaviorSubject(None)
            source_multicast = source.pipe(
                publish()
            )
            def unsubscribe_channel():
                if connection := socket_bs.value:
                    connection.send_json(unsubscribe_request_message)

            # Sender will trigger when calling the caller; when that happens, we want to send request to websocket
            source_multicast.subscribe(socket_bs)

            sub = CompositeDisposable(
                messages.pipe(
                    filter_channel_messages(channel),
                    operators.filter(lambda x: True if not pair else x[3] == pair),
                ).subscribe(observer),
                source_multicast.subscribe(on_next=on_socket_emitted),
                Disposable(action=unsubscribe_channel),
            )
            source_multicast.connect()
            return sub

        return Observable(observable_subscribe)
    return _subscribe
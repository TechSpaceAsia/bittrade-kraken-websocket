from typing import Tuple, Dict, List
from logging import getLogger

from reactivex import Observable, Observer, operators, Subject, interval
from reactivex.disposable import CompositeDisposable, Disposable
from reactivex.operators import take, publish
from reactivex.subject import BehaviorSubject

from bittrade_kraken_websocket.connection.generic import EnhancedWebsocket
from bittrade_kraken_websocket.events.events import EVENT_SUBSCRIBE, EVENT_UNSUBSCRIBE
from bittrade_kraken_websocket.events.request_response import request_response
from bittrade_kraken_websocket.messages.filters.kind import filter_channel_messages

logger = getLogger(__name__)


def subscribe_to_channel(messages: Observable[Dict | List], channel: str, pair: str = '', timeout=None):
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

    # sender is just a proxy; once everything is in place within the `caller` below, it will be triggered to send request message
    sender = Subject()
    # prepare a function that can be used to "call and wait for response", mimicking regular http API calls
    caller = request_response(sender, messages, timeout, EVENT_SUBSCRIBE, raise_on_status=True)

    def _subscribe(source: Observable[EnhancedWebsocket]):

        def on_socket_emitted(connection: EnhancedWebsocket):
            # We just map message to be sent through socket
            sender.pipe(
                take(1),
            ).subscribe(
                on_next=connection.send_json
            )
            # Everything is in place; trigger the call
            caller(request_message).subscribe()

        def observable_subscribe(observer: Observer, scheduler=None):
            socket_bs: BehaviorSubject = BehaviorSubject(None)
            source_multicast = source.pipe(
                publish()
            )

            def unsubscribe_channel():
                if connection := socket_bs.value:
                    connection.send_json(unsubscribe_request_message)

            # This allows us to keep the value of the latest socket to be used in unsubscribe; though there has to be a cleaner way
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

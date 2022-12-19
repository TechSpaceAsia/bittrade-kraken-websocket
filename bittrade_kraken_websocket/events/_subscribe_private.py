from typing import Literal, List, Dict, Tuple

from reactivex import Observable, Observer, operators, interval, Subject
from reactivex.disposable import CompositeDisposable
from reactivex.operators import replay

from bittrade_kraken_websocket.channels import CHANNEL_OPEN_ORDERS, CHANNEL_OWN_TRADES
from bittrade_kraken_websocket.connection.private import AuthenticatedWebsocket
from bittrade_kraken_websocket.connection.status import Status, WEBSOCKET_OPENED
from bittrade_kraken_websocket.events.events import EVENT_SUBSCRIBE
from bittrade_kraken_websocket.events.request_response import request_response
from bittrade_kraken_websocket.messages.filters.kind import filter_channel_messages

PrivateChannels = Literal[CHANNEL_OWN_TRADES, CHANNEL_OPEN_ORDERS]

def subscribe_private(messages: Observable[Dict | List], channel: PrivateChannels, timeout=None):
    timeout = timeout or interval(1.0)
    sender = Subject()
    caller = request_response(sender, messages, timeout, EVENT_SUBSCRIBE, raise_on_status=True)
    subscription_message = {
        "event": EVENT_SUBSCRIBE,
        "subscription": {
            "name": channel
        }
    }

    def on_authenticated_socket(message: Tuple[AuthenticatedWebsocket, Status]):
        ws, status = message
        if status == WEBSOCKET_OPENED:
            caller(subscription_message)

    def _subscribe_private(source: Observable[AuthenticatedWebsocket, Status]):
        def subscribe(observer: Observer, scheduler=None):
            recorded_messages = messages.pipe(
                filter_channel_messages(channel),
                operators.do_action(on_next=lambda x: print('YOOOO', x), on_completed=lambda: print('MEEEEEHHHHH')),
                replay()
            )

            # This subscription will get disposed by the "connect" sub below
            recorded_messages.subscribe(observer, scheduler=scheduler)

            sub = source.subscribe(on_next=on_authenticated_socket, scheduler=scheduler)

            return sub


        return Observable(subscribe)
    return _subscribe_private

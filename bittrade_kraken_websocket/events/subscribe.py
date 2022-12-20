from typing import Dict, List
from logging import getLogger

from reactivex import Observable, Observer, Subject, interval
from reactivex.disposable import CompositeDisposable
from reactivex.operators import take

from bittrade_kraken_websocket.connection.generic import EnhancedWebsocket
from bittrade_kraken_websocket.events.events import EVENT_SUBSCRIBE, EVENT_UNSUBSCRIBE
from bittrade_kraken_websocket.events.request_response import request_response
from bittrade_kraken_websocket.messages.filters.kind import keep_channel_messages

logger = getLogger(__name__)


def subscribe_to_channel(messages: Observable[Dict | List], channel: str, pair: str = '', timeout=None):
    timeout = timeout or interval(2.0)
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

    def _subscribe_to_channel(source: Observable[EnhancedWebsocket]):
        def observable_subscribe(observer: Observer, scheduler=None):
            def on_next(connection: EnhancedWebsocket):
                def send_to_connection(m):
                    logger.info('Sending subscription message to socket %s', m)
                    connection.send_json(m)
                sender.pipe(take(1)).subscribe(on_next=send_to_connection)
                caller(request_message).subscribe()
                sub.add(
                    messages.pipe(
                        keep_channel_messages(channel)
                    ).subscribe(
                        observer
                    )
                )
            sub = CompositeDisposable(
                source.subscribe(on_next=on_next)
            )
            return sub

        return Observable(observable_subscribe)

    return _subscribe_to_channel

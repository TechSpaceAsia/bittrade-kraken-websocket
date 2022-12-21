import functools
import time
from typing import Dict, List, Callable
from logging import getLogger

import reactivex
from reactivex import Observable, Observer, Subject, interval, operators, compose
from reactivex.disposable import CompositeDisposable, SerialDisposable, SingleAssignmentDisposable
from reactivex.operators import take

from bittrade_kraken_websocket.connection.generic import EnhancedWebsocket
from bittrade_kraken_websocket.events import ids
from bittrade_kraken_websocket.events.events import EVENT_SUBSCRIBE, EVENT_UNSUBSCRIBE
from bittrade_kraken_websocket.events.request_response import request_response, build_matcher, wait_for_response
from bittrade_kraken_websocket.messages.filters.kind import keep_channel_messages
from bittrade_kraken_websocket.messages.sequence import in_sequence, repeat_on_invalid_sequence

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
    caller = request_response(sender, messages, timeout)

    def _subscribe_to_channel(source: Observable[EnhancedWebsocket]):
        def subscribe(observer: Observer, scheduler=None):
            inner_subscription = SerialDisposable()
            def on_next(connection: EnhancedWebsocket):
                d = SingleAssignmentDisposable()
                inner_subscription.disposable = d
                d.disposable = messages.pipe(
                    keep_channel_messages(channel),
                    in_sequence(),
                    repeat_on_invalid_sequence(reactivex.from_callable(lambda: connection.send_json(unsubscribe_request_message)))
                ).subscribe(
                    observer
                )
                def send_to_connection(m):
                    logger.info('Sending subscription message to socket %s', m)
                    connection.send_json(m)
                sender.pipe(take(1)).subscribe(on_next=send_to_connection)
                caller(request_message).subscribe()

            sub = CompositeDisposable(
                source.subscribe(on_next=on_next, on_error=observer.on_error),
                inner_subscription
            )
            return sub

        return Observable(subscribe)

    return _subscribe_to_channel


# def websocket_to_messages(messages: Observable[Dict | List], socket: EnhancedWebsocket, id_generator: Observable[int]):
#     return request_response_factory(
#         on_enter=lambda x: socket.send_json(x),
#         id_generator=id_generator,
#         messages=messages,
#         timeout=reactivex.interval(3)
#     ).pipe(
#         operators.skip(1),
#         operators.concat(
#             messages
#         )
#     )

def subscribe_to_channel_v2(messages: Observable[Dict | List], channel: str, pair: str='', id_generator=None, timeout=None):
    """Note that at this level, messages needs to include all messages, not only dict (event stuff) or list (channel stuff) since we need both"""
    timeout = timeout or reactivex.interval(5)
    id_generator = id_generator or ids.id_generator
    subscription_message = {
        "event": EVENT_SUBSCRIBE,
        "subscription": {
            "name": channel
        }
    }
    if pair:
        subscription_message['pair'] = [pair]
    unsubscription_message = dict(subscription_message)
    unsubscription_message['event'] = EVENT_UNSUBSCRIBE
    def socket_to_channel_messages(socket: EnhancedWebsocket):
        do_this_on_invalid_sequence = reactivex.from_callable(functools.partial(socket.send_json, unsubscription_message))
        return request_response_factory(
            on_enter=lambda x: socket.send_json(dict(reqid=x, **subscription_message)),
            id_generator=id_generator,
            timeout=timeout,
            messages=messages
        ).pipe(
            # Don't care about the response message here?
            operators.skip(1),
            operators.concat(
                messages.pipe(
                    keep_channel_messages(channel),
                    in_sequence(),
                    operators.do_action(lambda x: print('3', x)),
                    repeat_on_invalid_sequence(do_this_on_invalid_sequence),
                    operators.do_action(lambda x: print('4', x)),
                )
            )
        )

    return compose(
        operators.map(socket_to_channel_messages),
        operators.switch_latest()
    )


def request_response_factory(on_enter: Callable[[int], None], id_generator: Observable[int], messages: Observable[Dict], timeout: Observable):
    def subscribe(observer: Observer, scheduler=None):
        message_id = id_generator.pipe(take(1)).run()
        sub = messages.pipe(
            wait_for_response(message_id, timeout),
            take(1)
        ).subscribe(
            on_error=observer.on_error,
            on_next=observer.on_next,
            on_completed=observer.on_completed,
            scheduler=scheduler
        )
        on_enter(message_id)
        return sub
    return Observable(subscribe)


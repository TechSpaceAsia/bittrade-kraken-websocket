import functools
import time
from typing import Dict, List, Callable
from logging import getLogger

import reactivex
from reactivex import Observable, Observer, Subject, interval, operators, compose
from reactivex.disposable import CompositeDisposable, SerialDisposable, SingleAssignmentDisposable, Disposable
from reactivex.operators import take

from bittrade_kraken_websocket.connection.generic import EnhancedWebsocket
from bittrade_kraken_websocket.development import debug_observer
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
                    repeat_on_invalid_sequence(
                        reactivex.from_callable(lambda: connection.send_json(unsubscribe_request_message)))
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

def channel_messages(messages: Observable[Dict | List], channel: str, socket: EnhancedWebsocket):
    def subscribe(observer: Observer, scheduler=None):
        socket.send_json({"event": EVENT_SUBSCRIBE, "subscription": {"name": channel}})
        last_sequence = [0]

        def on_next(message):
            if type(message) == list and len(message) > 2:
                _, c, sequence = message
                if c == channel:
                    new_sequence = sequence['sequence']
                    if new_sequence == last_sequence[0] + 1:
                        last_sequence[0] = new_sequence
                        observer.on_next(message)
                    else:
                        observer.on_error()

        messages_sub = messages.subscribe(
            on_next=on_next, scheduler=scheduler
        )

        def unsub():
            socket.send_json({"event": EVENT_UNSUBSCRIBE, "subscription": {"name": channel}})

        return CompositeDisposable(
            messages_sub,
            Disposable(action=unsub)
        )

    return Observable(subscribe)


def subscribe_to_channel_v3(messages: Observable[Dict | List], channel: str):
    return compose(
        operators.map(lambda socket: channel_messages(messages, channel, socket))
    )


def subscribe_to_private_channel(messages: Observable[Dict | List], channel: str, id_generator=None,
                                 timeout=None):
    """Note that at this level, messages needs to include all messages, not only dict (event stuff) or list (channel stuff) since we need both"""
    timeout = timeout or reactivex.interval(5)
    id_generator = id_generator or ids.id_generator
    subscription_message = {
        "event": EVENT_SUBSCRIBE,
        "subscription": {
            "name": channel
        }
    }
    unsubscription_message = dict(subscription_message)
    unsubscription_message['event'] = EVENT_UNSUBSCRIBE

    def socket_to_channel_messages(socket: EnhancedWebsocket):
        def on_exit():
            logger.debug('[SOCKET] Triggering on exit')
            socket.send_json(
                unsubscription_message
            )

        do_this_on_invalid_sequence = reactivex.from_callable(on_exit).pipe(
            operators.ignore_elements(),
        )

        def on_enter(x):
            socket.send_json(dict(reqid=x, **subscription_message))

        return request_response_factory(
            on_enter=on_enter,
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
                )
            ),
            repeat_on_invalid_sequence(
                do_this_on_invalid_sequence
            ),
        )

    return compose(
        operators.map(socket_to_channel_messages),
        operators.switch_latest(),
    )


def request_response_factory(on_enter: Callable[[int], None], id_generator: Observable[int], messages: Observable[Dict],
                             timeout: Observable):
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

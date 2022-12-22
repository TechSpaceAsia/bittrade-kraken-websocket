import functools
import time
from typing import Dict, List, Callable, TypeVar, Optional
from logging import getLogger

import reactivex
from reactivex import Observable, Observer, Subject, interval, operators, compose
from reactivex.disposable import CompositeDisposable, SerialDisposable, SingleAssignmentDisposable, Disposable
from reactivex.operators import take
from reactivex.scheduler.scheduler import Scheduler

from bittrade_kraken_websocket.channels import CHANNEL_OPEN_ORDERS, CHANNEL_OWN_TRADES
from bittrade_kraken_websocket.channels.models.message import PublicMessage
from bittrade_kraken_websocket.connection.generic import EnhancedWebsocket
from bittrade_kraken_websocket.development import debug_observer
from bittrade_kraken_websocket.events import ids
from bittrade_kraken_websocket.events.events import EVENT_SUBSCRIBE, EVENT_UNSUBSCRIBE
from bittrade_kraken_websocket.events.request_response import request_response, build_matcher, wait_for_response
from bittrade_kraken_websocket.messages.filters.kind import keep_channel_messages
from bittrade_kraken_websocket.messages.sequence import in_sequence, retry_on_invalid_sequence

logger = getLogger(__name__)


def subscribe_to_channel(messages: Observable[Dict | List], channel: str, pair: str = '', timeout=None):
    timeout = timeout or reactivex.timer(2.0)
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
                    retry_on_invalid_sequence(
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


def channel_subscription(socket: EnhancedWebsocket, channel: str, pair: str = '', subscription_kwargs: Dict = None):
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

    # Now that we created the unsub message, add extra things if provided
    if subscription_kwargs:
        subscription_message['subscription'].update(subscription_kwargs)

    def on_enter():
        socket.send_json(subscription_message)

    def on_exit():
        socket.send_json(unsubscription_message)

    def _channel_subscription(source: Observable[List]):
        def subscribe(observer: Observer, scheduler=None):
            on_enter()
            return CompositeDisposable(
                source.subscribe(observer, scheduler=scheduler),
                Disposable(action=on_exit)
            )

        return Observable(subscribe)

    return _channel_subscription


def subscribe_to_channel(messages: Observable[Dict | List], channel: str, *, pair: str = '',
                         subscription_kwargs: Dict = None):
    is_private = channel in (CHANNEL_OWN_TRADES, CHANNEL_OPEN_ORDERS)

    messages_operators = [in_sequence(), retry_on_invalid_sequence()] if is_private else []

    def socket_to_channel_messages(socket: EnhancedWebsocket) -> Observable[PublicMessage]:
        return messages.pipe(
            keep_channel_messages(channel, pair),
            channel_subscription(
                socket, channel, pair, subscription_kwargs
            ),
            *messages_operators
        )

    return compose(
        operators.map(socket_to_channel_messages),
        operators.switch_latest(),
        operators.share()
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

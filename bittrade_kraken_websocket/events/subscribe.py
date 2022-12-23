import functools
import time
from typing import Dict, List, Callable, Optional
from logging import getLogger

import reactivex
from reactivex import Observable, Observer, Subject, operators, compose
from reactivex.abc import ObserverBase, SchedulerBase
from reactivex.disposable import CompositeDisposable, SerialDisposable, SingleAssignmentDisposable, Disposable
from reactivex.operators import take

from bittrade_kraken_websocket.channels import ChannelName
from bittrade_kraken_websocket.channels.models.message import PublicMessage
from bittrade_kraken_websocket.connection.generic import EnhancedWebsocket
from bittrade_kraken_websocket.events import EventName, SubscriptionRequestMessage
from bittrade_kraken_websocket.events.request_response import request_response, build_matcher, wait_for_response
from bittrade_kraken_websocket.messages.filters.kind import keep_channel_messages
from bittrade_kraken_websocket.messages.sequence import in_sequence, retry_on_invalid_sequence

logger = getLogger(__name__)


def channel_subscription(socket: EnhancedWebsocket, channel: str, pair: str = '', subscription_kwargs: Dict = None):
    subscription_message: SubscriptionRequestMessage = {
        "event": EventName.EVENT_SUBSCRIBE,
        "subscription": {
            "name": channel
        }
    }
    if pair:
        subscription_message['pair'] = [pair]
    unsubscription_message: SubscriptionRequestMessage = dict(subscription_message)
    unsubscription_message['event'] = EventName.EVENT_UNSUBSCRIBE

    # Now that we created the unsub message, add extra things to the subscribe if provided
    # TODO check whether unsub requires those same arguments
    if subscription_kwargs:
        subscription_message['subscription'].update(subscription_kwargs)

    def on_enter():
        socket.send_json(subscription_message)

    def on_exit():
        socket.send_json(unsubscription_message)

    def _channel_subscription(source: Observable[List]):
        def subscribe(observer: ObserverBase, scheduler: Optional[SchedulerBase] = None):
            on_enter()
            return CompositeDisposable(
                source.subscribe(observer, scheduler=scheduler),
                Disposable(action=on_exit)
            )

        return Observable(subscribe)

    return _channel_subscription


def subscribe_to_channel(messages: Observable[Dict | List], channel: str, *, pair: str = '',
                         subscription_kwargs: Dict = None):
    is_private = channel in (ChannelName.CHANNEL_OWN_TRADES, ChannelName.CHANNEL_OPEN_ORDERS)

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


def request_response_factory(send_request: Callable[[int], None], id_generator: Observable[int],
                             messages: Observable[Dict],
                             timeout: float):
    """
    A factory to create an observable which behaves like a request/response:
    on subscribe it calls `send_request` with the id of the request as argument.
    when a message is found in the `messages` stream that matches the id, it is emitted and the observable then completes

    :param send_request: A function that should send out the request on subscribe. Typically lambda request_id: websocket.send_json(...)
    :param id_generator: An observable sequence of ids. Defaults to ids.id_generator which provides a (pretty unique) uuid1 based int. `.run()` is used on it so it will block the current thread
    :param messages: The observable sequence of socket messages. It must contain at least the event messages (so cannot be limited to channel messages)
    :param timeout: Timeout in seconds
    :return:
    """

    def subscribe(observer: ObserverBase, scheduler: Optional[SchedulerBase] = None):
        message_id = id_generator.pipe(take(1)).run()
        sub = messages.pipe(
            wait_for_response(message_id, timeout),
        ).subscribe(
            on_error=observer.on_error,
            on_next=observer.on_next,
            on_completed=observer.on_completed,
            scheduler=scheduler
        )
        send_request(message_id)
        return sub

    return Observable(subscribe)

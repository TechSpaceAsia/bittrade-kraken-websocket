from logging import getLogger
from typing import Dict, List, Optional

from reactivex import Observable, operators, compose
from reactivex.abc import ObserverBase, SchedulerBase
from reactivex.disposable import CompositeDisposable, Disposable

from bittrade_kraken_websocket.channels import ChannelName
from bittrade_kraken_websocket.channels.models.message import PublicMessage
from bittrade_kraken_websocket.connection.generic import EnhancedWebsocket
from bittrade_kraken_websocket.events import EventName, SubscriptionRequestMessage
from bittrade_kraken_websocket.messages.filters.kind import keep_channel_messages
from bittrade_kraken_websocket.messages.sequence import in_sequence, retry_on_invalid_sequence

logger = getLogger(__name__)


def channel_subscription(socket: EnhancedWebsocket, channel: ChannelName, pair: str = '', subscription_kwargs: Dict = None):
    subscription_message: SubscriptionRequestMessage = {
        "event": EventName.EVENT_SUBSCRIBE,
        "subscription": {
            "name": channel
        }
    }
    if pair:
        subscription_message['pair'] = [pair]
    if subscription_kwargs:
        subscription_message['subscription'].update(subscription_kwargs)

    unsubscription_message: SubscriptionRequestMessage = dict(subscription_message)
    unsubscription_message['event'] = EventName.EVENT_UNSUBSCRIBE

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


def subscribe_to_channel(messages: Observable[Dict | List], channel: ChannelName, *, pair: str = '',
                         subscription_kwargs: Dict = None):
    is_private = channel in (ChannelName.CHANNEL_OWN_TRADES, ChannelName.CHANNEL_OPEN_ORDERS)

    messages_operators = [in_sequence(), retry_on_invalid_sequence()] if is_private else []

    def socket_to_channel_messages(socket: EnhancedWebsocket) -> Observable[PublicMessage]:
        return messages.pipe(
            keep_channel_messages(channel, pair),
            channel_subscription(
                socket, channel, pair, subscription_kwargs
            ),
            *messages_operators,
        )

    return compose(
        operators.map(socket_to_channel_messages),
        operators.switch_latest(),
        operators.share(),
    )
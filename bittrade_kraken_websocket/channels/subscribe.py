from logging import getLogger
from typing import Callable, Dict, List, Optional
import typing

from reactivex import Observable, operators, compose
from reactivex.abc import ObserverBase, SchedulerBase
from reactivex.disposable import CompositeDisposable, Disposable

from .channels import ChannelName
from .models.message import (
    PrivateMessage,
    PublicMessage,
)
from bittrade_kraken_websocket.connection.generic import EnhancedWebsocket
from bittrade_kraken_websocket.events import EventName, SubscriptionRequestMessage
from bittrade_kraken_websocket.messages.filters.kind import keep_channel_messages
from bittrade_kraken_websocket.messages.sequence import (
    in_sequence,
    retry_on_invalid_sequence,
)

logger = getLogger(__name__)


def channel_subscription(
    socket: EnhancedWebsocket,
    channel: ChannelName,
    pair: str = "",
    subscription_kwargs: Optional[Dict[str, str]] = None,
):
    subscription_message: SubscriptionRequestMessage = {
        "event": EventName.EVENT_SUBSCRIBE,
        "subscription": {"name": channel.value},
    }  # type: ignore  We have to ignore because NotRequired (a feature of TypedDict) is currently not available so "pair" is always required
    if pair:
        subscription_message["pair"] = [pair]
    if subscription_kwargs:
        subscription_message["subscription"].update(subscription_kwargs)

    unsubscription_message = dict(subscription_message)
    unsubscription_message["event"] = EventName.EVENT_UNSUBSCRIBE

    def on_enter():
        socket.send_json(typing.cast(Dict, subscription_message))

    def on_exit():
        try:
            socket.send_json(typing.cast(Dict, unsubscription_message))
        except Exception as ex:
            logger.error("Could not send unsubscribe messages: %s", ex)

    def _channel_subscription(source: Observable[List]):
        def subscribe(
            observer: ObserverBase, scheduler: Optional[SchedulerBase] = None
        ):
            on_enter()
            return CompositeDisposable(
                source.subscribe(observer, scheduler=scheduler),
                Disposable(action=on_exit),
            )

        return Observable(subscribe)

    return _channel_subscription


def subscribe_to_channel(
    messages: Observable[Dict | List],
    channel: ChannelName,
    *,
    pair: str = "",
    subscription_kwargs: Optional[Dict] = None,
) -> Callable[
    [Observable[EnhancedWebsocket]], Observable[PublicMessage | PrivateMessage]
]:
    is_private = channel in (
        ChannelName.CHANNEL_OWN_TRADES,
        ChannelName.CHANNEL_OPEN_ORDERS,
    )
    subscription_keywords: Dict = subscription_kwargs or {}
    messages_operators = []
    if is_private:
        messages_operators += [in_sequence(), retry_on_invalid_sequence()]

    def socket_to_channel_messages(
        socket: EnhancedWebsocket,
    ) -> Observable[PublicMessage | PrivateMessage]:
        return messages.pipe(
            keep_channel_messages(channel, pair),
            channel_subscription(socket, channel, pair, subscription_keywords),
            *messages_operators,
        )

    return compose(
        operators.map(socket_to_channel_messages),
        operators.switch_latest(),
        operators.share(),
    )

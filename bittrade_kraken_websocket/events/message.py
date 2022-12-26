from typing import List, TypedDict

from bittrade_kraken_websocket.channels import ChannelName
from bittrade_kraken_websocket.events import EventName


class Subscription(TypedDict):
    name: ChannelName


class RequestMessage(TypedDict):
    event: EventName


class SubscriptionRequestMessage(TypedDict):
    event: EventName
    pair: List[str]  # will eventually use NotRequired
    subscription: Subscription

import dataclasses
from typing import List, Optional, TypedDict

from bittrade_kraken_websocket.events import EventName


class Subscription(TypedDict):
    name: str


class RequestMessage(TypedDict):
    event: EventName


class SubscriptionRequestMessage(TypedDict):
    event: EventName
    pair: Optional[List[str]]
    subscription: Subscription


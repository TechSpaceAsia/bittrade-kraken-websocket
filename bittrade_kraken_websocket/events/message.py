import dataclasses
from typing import List, Optional

from bittrade_kraken_websocket.events.events import EventType


@dataclasses.dataclass
class RequestMessage:
    event: EventType

@dataclasses.dataclass
class SubscriptionRequestMessage:
    event: EventType
    pair: Optional[List[str]] = None

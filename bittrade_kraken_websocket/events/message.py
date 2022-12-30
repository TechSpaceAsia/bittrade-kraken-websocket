from typing import Dict, List, TypedDict

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .events import EventName



class RequestMessage(TypedDict):
    event: "EventName"


class SubscriptionRequestMessage(TypedDict):
    event: "EventName"
    pair: List[str]  # will eventually use NotRequired
    subscription: Dict[str, str]

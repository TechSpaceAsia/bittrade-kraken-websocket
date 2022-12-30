from typing import List, Dict, Any

from reactivex import Observable, compose, operators

from .models.message import PrivateMessage, PublicMessage

from .channels import ChannelName
from .models.spread import SpreadPayload
from .payload import to_payload
from .subscribe import subscribe_to_channel


def to_spread_payload(message: PrivateMessage | PublicMessage):
    return to_payload(message, SpreadPayload)


def subscribe_spread(pair: str, messages: Observable[Dict | List]):
    return compose(
        subscribe_to_channel(messages, ChannelName.CHANNEL_TICKER, pair=pair),
        operators.map(to_spread_payload),
    )

__all__ = [
    "SpreadPayload",
    "subscribe_spread",
]
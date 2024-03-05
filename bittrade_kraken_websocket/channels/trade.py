from typing import List, Dict, Any

from reactivex import Observable, compose, operators

from .models.message import PrivateMessage, PublicMessage

from .channels import ChannelName
from .models.trade import TradePayload
from .payload import to_payload
from .subscribe import subscribe_to_channel


def to_trade_payload(message: PrivateMessage | PublicMessage):
    return [TradePayload(*payload) for payload in message]


def subscribe_trade(pair: str, messages: Observable[Dict | List]):
    return compose(
        subscribe_to_channel(messages, ChannelName.CHANNEL_TRADE, pair=pair),
        operators.map(lambda x: x[1]),
        operators.map(to_trade_payload),
    )

__all__ = [
    "TradePayload",
    "subscribe_trade",
]
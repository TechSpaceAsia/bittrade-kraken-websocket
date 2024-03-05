from typing import List, Dict, Any

from reactivex import Observable, compose, operators

from .models.message import PrivateMessage, PublicMessage

from .channels import ChannelName
from .models.ohlc import OHLCPayload
from .subscribe import subscribe_to_channel


def to_ohlc_payload(message):
    return OHLCPayload(*message[1]) 


def subscribe_ohlc(pair: str, messages: Observable[Dict | List], interval: int=1):
    return compose(
        subscribe_to_channel(messages, ChannelName.CHANNEL_OHLC, pair=pair, subscription_kwargs={"interval": interval}),
        operators.map(to_ohlc_payload),
    )

__all__ = [
    "OHLCPayload",
    "subscribe_ohlc",
]
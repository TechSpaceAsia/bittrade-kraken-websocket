from typing import List, Dict

from reactivex import Observable, compose, operators

from bittrade_kraken_websocket.channels import ChannelName
from bittrade_kraken_websocket.channels.models.message import PrivateMessage, PublicMessage
from bittrade_kraken_websocket.channels.models.ticker import TickerPayload
from bittrade_kraken_websocket.channels.payload import to_payload
from bittrade_kraken_websocket.channels.subscribe import subscribe_to_channel


def to_ticker_payload(message: PrivateMessage | PublicMessage):
    return to_payload(message, TickerPayload)


def subscribe_ticker(pair: str, messages: Observable[Dict | List]):
    return compose(
        subscribe_to_channel(messages, ChannelName.CHANNEL_TICKER, pair=pair),
        operators.map(to_ticker_payload),
    )

__all__ = [
    "TickerPayload",
    "subscribe_ticker",
]
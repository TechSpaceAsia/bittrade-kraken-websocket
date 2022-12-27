from typing import List, Dict

from reactivex import Observable, compose, operators

from bittrade_kraken_websocket.channels import ChannelName
from bittrade_kraken_websocket.channels.models.own_trades import OwnTradesPayload, OwnOwnTradesPayloadEntryTrade
from bittrade_kraken_websocket.channels.payload import private_to_payload
from bittrade_kraken_websocket.channels.subscribe import subscribe_to_channel


def to_own_trades_payload(message: List):
    return private_to_payload(message, OwnTradesPayload)


def subscribe_own_trades(pair: str, messages: Observable[Dict | List]):
    return compose(
        subscribe_to_channel(messages, ChannelName.CHANNEL_OWN_TRADES),
        operators.map(to_own_trades_payload),
    )

__all__ = [
    "OwnTradesPayload",
    "subscribe_own_trades",
    "OwnOwnTradesPayloadEntryTrade",
]
from decimal import Decimal
from typing import List, Dict, Optional, TypedDict, Any, cast

from reactivex import Observable, compose, operators

from bittrade_kraken_websocket.channels import ChannelName
from bittrade_kraken_websocket.channels.models.message import PrivateMessage
from bittrade_kraken_websocket.channels.payload import private_to_payload
from bittrade_kraken_websocket.channels.subscribe import subscribe_to_channel
from bittrade_kraken_websocket.events import OrderSide, OrderType
from pydantic.dataclasses import dataclass


class OwnTradesPayloadEntry(TypedDict):
    """
      [
      {
        "TDLH43-DVQXD-2KHVYY": {
          "cost": "1000000.00000",
          "fee": "1600.00000",
          "margin": "0.00000",
          "ordertxid": "TDLH43-DVQXD-2KHVYY",
          "ordertype": "limit",
          "pair": "XBT/EUR",
          "postxid": "OGTT3Y-C6I3P-XRI6HX",
          "price": "100000.00000",
          "time": "1560516023.070651",
          "type": "sell",
          "vol": "1000000000.00000000"
        }
      },
      {
        "TDLH43-DVQXD-2KHVYY": {
          "cost": "1000000.00000",
          "fee": "600.00000",
          "margin": "0.00000",
          "ordertxid": "TDLH43-DVQXD-2KHVYY",
          "ordertype": "limit",
          "pair": "XBT/EUR",
          "postxid": "OGTT3Y-C6I3P-XRI6HX",
          "price": "100000.00000",
          "time": "1560516023.070658",
          "type": "buy",
          "vol": "1000000000.00000000"
        }
      },
      {
        "TDLH43-DVQXD-2KHVYY": {
          "cost": "1000000.00000",
          "fee": "1600.00000",
          "margin": "0.00000",
          "ordertxid": "TDLH43-DVQXD-2KHVYY",
          "ordertype": "limit",
          "pair": "XBT/EUR",
          "postxid": "OGTT3Y-C6I3P-XRI6HX",
          "price": "100000.00000",
          "time": "1560520332.914657",
          "type": "sell",
          "vol": "1000000000.00000000"
        }
      },
      {
        "TDLH43-DVQXD-2KHVYY": {
          "cost": "1000000.00000",
          "fee": "600.00000",
          "margin": "0.00000",
          "ordertxid": "TDLH43-DVQXD-2KHVYY",
          "ordertype": "limit",
          "pair": "XBT/EUR",
          "postxid": "OGTT3Y-C6I3P-XRI6HX",
          "price": "100000.00000",
          "time": "1560520332.914664",
          "type": "buy",
          "vol": "1000000000.00000000"
        }
      }
    ]
    """

    cost: str
    fee: str
    margin: str
    ordertxid: str
    ordertype: str
    pair: str
    postxid: str
    price: str
    time: str
    type: str
    vol: str

@dataclass
class OwnTradesPayloadParsed:
    cost: Decimal
    fee: str
    margin: str
    ordertxid: str
    ordertype: OrderType
    pair: str
    postxid: str
    price: Decimal
    time: str
    type: OrderSide
    vol: Decimal

OwnTradesPayload = List[Dict[str, OwnTradesPayloadEntry]]


def to_own_trades_payload(message: PrivateMessage):
    return private_to_payload(message, OwnTradesPayload)


def parse_own_trade(payload: OwnTradesPayloadEntry):
    return OwnTradesPayloadParsed(**payload)


def subscribe_own_trades(
    messages: Observable[Dict | List], subscription_kwargs: Optional[Dict] = None
):
    """Subscribe to list of own trades
    By default, we skip the first message each time we have to resubscribe because:
        > On subscription last 50 trades for the user will be sent, followed by new trades.
    However trades don't get updated so this snapshot feels inconsistent with other feeds

    Set your own subscription_kwargs to avoid that behavior
    """
    subscription_kwargs = subscription_kwargs or {"snapshot": False}
    return compose(
        subscribe_to_channel(
            messages,
            ChannelName.CHANNEL_OWN_TRADES,
            subscription_kwargs=subscription_kwargs,
        ),
        operators.map(lambda x: to_own_trades_payload(cast(PrivateMessage, x))),
    )


__all__ = [
    "OwnTradesPayload",
    "subscribe_own_trades",
    "OwnTradesPayloadEntry",
    "parse_own_trade",
    "OwnTradesPayloadParsed",
]

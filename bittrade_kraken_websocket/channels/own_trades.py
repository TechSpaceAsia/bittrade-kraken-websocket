from typing import List, Dict, TypedDict

from reactivex import Observable, compose, operators

from bittrade_kraken_websocket.channels import ChannelName
from bittrade_kraken_websocket.channels.payload import private_to_payload
from bittrade_kraken_websocket.channels.subscribe import subscribe_to_channel


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


OwnTradesPayload = List[Dict[str, OwnTradesPayloadEntry]]


def to_own_trades_payload(message: List):
    return private_to_payload(message, OwnTradesPayload)


def subscribe_own_trades(messages: Observable[Dict | List], skip_first=True):
    """Subscribe to list of own trades
    By default, we skip the first message each time we have to resubscribe because:
        > On subscription last 50 trades for the user will be sent, followed by new trades.
    However trades don't get updated so this snapshot feels inconsistent with other feeds

    Set skip_first to False to include the initial payload
    """
    return compose(
        subscribe_to_channel(
            messages, ChannelName.CHANNEL_OWN_TRADES, skip_first=skip_first
        ),
        operators.map(to_own_trades_payload),
    )


__all__ = [
    "OwnTradesPayload",
    "subscribe_own_trades",
    "OwnTradesPayloadEntry",
]

from typing import List, Dict, Literal, TypedDict

from reactivex import Observable, compose, operators

from bittrade_kraken_websocket.channels import ChannelName
from bittrade_kraken_websocket.channels.payload import private_to_payload
from bittrade_kraken_websocket.channels.subscribe import subscribe_to_channel

OrderType = Literal["buy", "sell"]


class OpenOrdersPayloadEntryDescr(TypedDict):
    close: str
    leverage: str
    order: str
    ordertype: str
    pair: str
    price: str
    price2: str
    type: OrderType


class OpenOrdersPayloadEntry(TypedDict):
    avg_price: str
    cost: str
    descr: OpenOrdersPayloadEntryDescr | str
    expiretm: str
    fee: str
    limitprice: str
    misc: str
    oflags: str
    opentm: str
    refid: str
    starttm: str
    status: str
    stopprice: str
    timeinforce: str
    userref: int
    vol: str
    vol_exec: str


"""
Sample
    {
            "OHOCUM-KM3UM-6Y7GPI": {
                "avg_price": "0.00000000",
                "cost": "0.00000000",
                "descr": {
                    "close": null,
                    "leverage": null,
                    "order": "buy 30.00000000 USDT/USD @ limit 0.99980000",
                    "ordertype": "limit",
                    "pair": "USDT/USD",
                    "price": "0.99980000",
                    "price2": "0.00000000",
                    "type": "buy"
                },
                "expiretm": null,
                "fee": "0.00000000",
                "limitprice": "0.00000000",
                "misc": "",
                "oflags": "fciq",
                "opentm": "1672114415.357414",
                "refid": null,
                "starttm": null,
                "status": "pending",
                "stopprice": "0.00000000",
                "timeinforce": "GTC",
                "userref": 0,
                "vol": "30.00000000",
                "vol_exec": "0.00000000"
            }
        }
    ]
"""


OpenOrdersPayload = List[Dict[str, OpenOrdersPayloadEntry]]


def to_open_orders_payload(message: List):
    return private_to_payload(message, OpenOrdersPayload)


def subscribe_open_orders(messages: Observable[Dict | List]):
    return compose(
        subscribe_to_channel(messages, ChannelName.CHANNEL_OPEN_ORDERS),
        operators.map(to_open_orders_payload),
    )


__all__ = [
    "OpenOrdersPayload",
    "subscribe_open_orders",
    "OpenOrdersPayloadEntry",
    "OpenOrdersPayloadEntryDescr",
]

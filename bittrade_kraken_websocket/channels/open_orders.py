from typing import List, Dict, Literal, TypedDict

from reactivex import Observable, compose, operators

from .channels import ChannelName
from .payload import private_to_payload
from .subscribe import subscribe_to_channel

from bittrade_kraken_websocket.events import Order, OrderStatus

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


def is_partial_fill_update(message: OpenOrdersPayloadEntry):
    """
    Messages like this mean partial fill of an order
    {
        "OKUIN4-EZVJ2-DTQYZV": {
          "vol_exec": "33.46899999",
          "cost": "33.45895929",
          "fee": "0.00000000",
          "avg_price": "0.99970000",
          "userref": 0
        }
      }
    """
    return "status" not in message


def is_initial_details(message: OpenOrdersPayloadEntry):
    """
    These messages represent initial acknowledgment and details
    {
        "OIEAGC-QXXOL-KWFCG4": {
          "avg_price": "0.00000000",
          "cost": "0.00000000",
          "descr": {
            "close": null,
            "leverage": null,
            "order": "sell 295.56960000 USDT/USD @ limit 0.99970000",
            "ordertype": "limit",
            "pair": "USDT/USD",
            "price": "0.99970000",
            "price2": "0.00000000",
            "type": "sell"
          },
          "expiretm": null,
          "fee": "0.00000000",
          "limitprice": "0.00000000",
          "misc": "",
          "oflags": "fciq",
          "opentm": "1672348988.827044",
          "refid": null,
          "starttm": null,
          "status": "pending",
          "stopprice": "0.00000000",
          "timeinforce": "GTC",
          "userref": 0,
          "vol": "295.56960000",
          "vol_exec": "0.00000000"
        }
      }
    """
    return message.get("status") == "pending"


def initial_details_to_order(message: OpenOrdersPayloadEntry, order_id=str) -> "Order":
    descr = message["descr"]
    return Order(
        order_id=order_id,
        status=OrderStatus.pending,
        description=descr["order"],
        price=descr["price"],
        volume=message["vol"],
        volume_executed=message["vol_exec"],
    )


def is_close_message(message: OpenOrdersPayloadEntry):
    return message.get("status") == "closed"


def is_cancel_message(message: OpenOrdersPayloadEntry):
    return message.get("status") == "canceled"


def is_final_message(message: OpenOrdersPayloadEntry):
    return is_close_message(message) or is_cancel_message(message)


def is_open_message(message: OpenOrdersPayloadEntry):
    return message.get("status") == "open"


__all__ = [
    "OpenOrdersPayload",
    "subscribe_open_orders",
    "OpenOrdersPayloadEntry",
    "OpenOrdersPayloadEntryDescr",
    "initial_details_to_order",
    "is_open_message",
    "is_final_message",
    "is_cancel_message",
    "is_close_message",
    "is_initial_details",
    "is_partial_fill_update",
]

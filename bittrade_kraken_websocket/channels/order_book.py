from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple, TypedDict, cast
from reactivex import Observable, compose, operators

from bittrade_kraken_websocket.channels.channels import ChannelName
from bittrade_kraken_websocket.channels.subscribe import subscribe_to_channel

class Price(TypedDict):
    price: str
    volume: str
    timestamp: str

RawPrice = Tuple[str, str, str, str]

@dataclass
class BookPrice:
    ask: Dict[str, Price]
    bid: Dict[str, Price]

InitialBookPrice = TypedDict('InitialBookPrice', {'as': List[RawPrice], 'bs': List[RawPrice]})

class UpdateBookPrice(TypedDict):
    a: List[RawPrice]
    b: List[RawPrice]

OrderBook = Tuple[int, InitialBookPrice | UpdateBookPrice, str, str]

def to_order_book_payload(payload: OrderBook):
    book_price_list = payload[1]
    if type(payload[2]) == dict:
        book_price_list.update(payload[2])
    # book_price = BookPrice(ask=dict(), bid=dict())
    # keys = ["as", "bs", "a", "b"]
    # for key in keys:
    #     for price in book_price_list.get(key, []):
    #         current_price = price[0]
    #         if key == "as" or key == "a":
    #             book_price.ask.update(
    #                 {
    #                     current_price: {
    #                         "price": price[0],
    #                         "volume": price[1],
    #                         "timestamp": price[2],
    #                     }
    #                 }
    #             )
    #         else:
    #             book_price.bid.update(
    #                 {
    #                     current_price: {
    #                         "price": price[0],
    #                         "volume": price[1],
    #                         "timestamp": price[2],
    #                     }
    #                 }
    #             )

    return book_price_list

def accumulate_book_price(acc: BookPrice, current: InitialBookPrice | UpdateBookPrice) -> BookPrice:
    ask_list = current.get("as") or current.get("a", [])
    bid_list = current.get("bs") or current.get("b", [])

    for item in ask_list:
        price = item[0]
        if float(item[1]):
            acc.ask.update({
                    price: {
                    "price": price,
                    "volume": item[1],
                    "timestamp": item[2],
                }
            })
        else:
            if price in acc.ask:
                acc.ask.pop(price)

    for item in bid_list:
        price = item[0]
        if float(item[1]):
            acc.bid.update(
                {
                    price: {
                        "price": price,
                        "volume": item[1],
                        "timestamp": item[2],
                    }
                }
            )
        else:
            if price in acc.bid:
                acc.bid.pop(price)

    acc.ask = dict(sorted(acc.ask.items(), key=lambda x: float(x[0]))[:10])
    acc.bid = dict(sorted(acc.bid.items(), key=lambda x: float(x[0]), reverse=True)[:10])

    return acc

def subscribe_order_book(
    pair: str, messages: Observable[Dict | List], subscription_kwargs: Optional[Dict] = None
):
    subscription_kwargs = subscription_kwargs or {"depth": 10}
    return compose(
        subscribe_to_channel(
            messages,
            ChannelName.CHANNEL_BOOK,
            subscription_kwargs=subscription_kwargs,
            pair=pair,
        ),
        operators.map(lambda x: to_order_book_payload(cast(OrderBook, x))),
        operators.scan(accumulate_book_price, BookPrice(ask=dict(), bid=dict())),
    )

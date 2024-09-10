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
    ask: List[Price]
    bid: List[Price]

InitialBookPrice = TypedDict('InitialBookPrice', {'as': List[RawPrice], 'bs': List[RawPrice]})

class UpdateBookPrice(TypedDict):
    a: List[RawPrice]
    b: List[RawPrice]

OrderBook = Tuple[int, InitialBookPrice | UpdateBookPrice, str, str]

def to_order_book_payload(payload: OrderBook):
    book_price_list = payload[1]
    book_price = BookPrice(ask=[], bid=[])
    keys = ["as", "bs", "a", "b"]
    for key in keys:
      for price in book_price_list.get(key, []):
          if key == "as" or key == "a":
              book_price.ask.append({"price": price[0], "volume": price[1], "timestamp": price[2]})
          else:
              book_price.bid.append({"price": price[0], "volume": price[1], "timestamp": price[2]})
    
    return book_price

def accumulate_book_price(acc: BookPrice, current: BookPrice) -> BookPrice:
    if current.ask and len(acc.ask):
        for price in current.ask:
            for ask_index, current_price in enumerate(acc.ask):
                if price["price"] == current_price["price"]:
                    if not float(price["volume"]):
                        acc.ask.pop(ask_index)
                        break
                    
                    acc.ask[ask_index] = price
                    break

                if price["price"] <= current_price["price"] and float(price["volume"]):
                    acc.ask.insert(ask_index, price)
                    break
    elif not len(acc.ask):
        acc.ask = current.ask
    
    if current.bid and len(acc.bid):
        for price in current.bid:
            for bid_index, current_price in enumerate(acc.bid):
                if price["price"] == current_price["price"]:
                    if not float(price["volume"]):
                        acc.bid.pop(bid_index)
                        break

                    acc.bid[bid_index] = price
                    break

                if price["price"] >= current_price["price"] and float(price["volume"]):
                    acc.bid.insert(bid_index, price)
                    break
    elif not len(acc.bid):
        acc.bid = current.bid

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
        operators.scan(accumulate_book_price, BookPrice(ask=[], bid=[])),
    )

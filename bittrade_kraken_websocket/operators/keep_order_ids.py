from typing import Callable, Dict, List, Set
from reactivex import compose, operators, Observable
from bittrade_kraken_websocket.channels.own_trades import to_own_trades_payload, OwnTradesPayload


def add_to_set(acc: Set[str], x: OwnTradesPayload):
    for orders in x:
        acc.update(orders.keys())
    return acc

def reduce_order_ids() -> Callable[[Observable[OwnTradesPayload]], Observable[Set[str]]]:
    """Takes an observable of ownOrders and keeps their ids in a set
    Useful for ignoring trades that may not be relevant
    """
    return compose(
        operators.scan(
            add_to_set, set()
        ),
        operators.distinct_until_changed(key_mapper=lambda x: len(x))
    )
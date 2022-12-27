from typing import Callable, Dict, List, Set
from reactivex import compose, operators, Observable
from bittrade_kraken_websocket.channels.payload import private_to_payload

def add_to_set(acc: Set[str], x: List[Dict]):
    for orders in x:
        acc.update(orders.keys())
    return acc

def reduce_order_ids() -> Callable[[Observable[List[Dict]]], Observable[Set[str]]]:
    return compose(
        operators.map(private_to_payload),
        operators.scan(
            add_to_set, set()
        ),
        operators.distinct_until_changed(key_mapper=lambda x: len(x))
    )
from typing import Dict, List, Tuple, TypedDict


class PrivateSequence(TypedDict):
    sequence: int


PrivateMessage = Tuple[List, str, PrivateSequence]

# Note that these don't match quite Orderbook update messages which may have 5 values
PublicMessage = Tuple[int, List | Dict, str, str]

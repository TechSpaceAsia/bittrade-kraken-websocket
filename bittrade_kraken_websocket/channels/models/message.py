from typing import Dict, List, Tuple, TypedDict


class PrivateSequence(TypedDict):
    sequence: int


PrivateMessage = Tuple[List, str, PrivateSequence]

# Note that these don't match Orderbook update messages
PublicMessage = Tuple[int, List | Dict, str, str]

from typing import NamedTuple, Literal

OHLCPayload = NamedTuple("OHLCPayload", [("time", str), ("etime", str), ("open",str), ("high", str), ("low", str), ("close", str), ("vwap", str), ("volume", str), ("count", int)])

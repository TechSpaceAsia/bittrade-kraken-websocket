from typing import NamedTuple, Literal

TradePayload = NamedTuple("TradePayload", [("price", str), ("volume", str), ("time",str), ("side", Literal["b", "s"]), ("orderType", Literal["m", "l"]), ("misc", str)])

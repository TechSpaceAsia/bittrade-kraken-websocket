from typing import NamedTuple, Literal

"""https://docs.kraken.com/websockets/#message-spread"""
TradePayload = NamedTuple("TradePayload", [("price", str), ("volume", str), ("time",str), ("side", Literal["b", "s"]), ("orderType", Literal["m", "l"]), ("misc", str)])

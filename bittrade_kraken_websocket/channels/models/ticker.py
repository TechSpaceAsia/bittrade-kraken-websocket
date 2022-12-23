from typing import NamedTuple, TypedDict, Tuple

Level = NamedTuple('Level', price=str, whole_lot_volume=int, lot_volume=str)

ShortLevel = NamedTuple('ShortLevel', price=str, lot_volume=str)

Volume = NamedTuple('Volume', today=str, last_24_hours=str)
Order = NamedTuple('Order', today=str, last_24_hours=str)

TradeVolume = NamedTuple('TradeVolume', today=int, last_24_hours=int)


class TickerPayload(TypedDict):
    """https://docs.kraken.com/websockets/#message-ticker"""
    a: Level  #ask
    b: Level  #bid
    c: ShortLevel #close
    v: Volume
    p: Volume # Volume weighted average price
    t: TradeVolume
    l: Order # low price
    h: Order # high price
    o: Order # open price

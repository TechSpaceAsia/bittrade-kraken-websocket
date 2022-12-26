from typing import Tuple, TypedDict

Level = Tuple[str, int, str] # in this order: price=str, whole_lot_volume=int, lot_volume=str

ShortLevel = Tuple[str, str] # price, lot_volume

Volume = Tuple[str, str] # today, last_24_hours
Order = Tuple[str, str] # today, last_24_hours

TradeVolume = Tuple[int, int] # today, last_24_hours


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

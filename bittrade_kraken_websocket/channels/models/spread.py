from typing import Tuple

"""https://docs.kraken.com/websockets/#message-spread"""
SpreadPayload = Tuple[str, str, str, str, str]  # bid, ask, timestamp, bidVolume, askVolume

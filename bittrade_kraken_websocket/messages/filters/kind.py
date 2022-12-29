from reactivex import operators
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from bittrade_kraken_websocket.channels import ChannelName


def _is_channel_message(channel: "ChannelName", pair: str = ""):
    # Channel messages have at least 3 length and come with second to last as channel name
    def func(x):
        if type(x) != list or len(x) < 3 or x[-2] != channel.value:
            return False
        if not pair:
            return True
        return pair == x[-1]

    return func


def keep_channel_messages(channel: "ChannelName", pair: str = ""):
    return operators.filter(_is_channel_message(channel, pair))

from typing import List

from reactivex import operators


def _is_channel_message(*channels):
    # Channel messages have at least 4 length and come with second to last as channel name
    def func(x):
        return type(x) == list and len(x) >= 4 and (x[-2] in channels if channels else True)

    return func


def keep_channel_messages(*channels):
    return operators.filter(_is_channel_message(*channels))



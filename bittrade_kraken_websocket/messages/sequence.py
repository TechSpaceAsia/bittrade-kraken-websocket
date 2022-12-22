from logging import getLogger
from typing import Callable, List, Tuple, TypeVar
from bittrade_kraken_websocket.channels.models import PrivateMessage

import reactivex
from reactivex import Observable, compose, operators

from bittrade_kraken_websocket.channels.models.message import PublicMessage

logger = getLogger(__name__)



class InvalidSequence(ValueError):
    pass


def correct_sequence_or_throw(x: Tuple[PrivateMessage, PrivateMessage]) -> PrivateMessage:
    previous, current = x
    expected = previous[2]['sequence'] + 1
    actual = current[2]['sequence']
    if expected == 1:  # means sequence was 0, meaning it's the first message
        logger.info('[SOCKET][SEQUENCE] Fresh start of sequence; starting from %s', actual)
        return current
    if actual == expected:
        return current
    logger.warning('[SOCKET][SEQUENCE] Invalid sequence; expected %s, got %s', expected, actual)
    raise InvalidSequence()


def in_sequence() -> Callable[[Observable[PrivateMessage]], Observable[PrivateMessage]]:
    return compose(
        operators.start_with([[], "", {"sequence": 0}]),
        operators.pairwise(),
        operators.map(correct_sequence_or_throw),
    )


def retry_on_invalid_sequence() -> Callable[[Observable[PublicMessage]], Observable[PublicMessage]]:
    """Retry on InvalidSequence error only
    This will allow to refresh the subscription
    """

    def on_error(exc, source):
        if type(exc) == InvalidSequence:
            return reactivex.empty()
        return reactivex.throw(exc)

    return compose(
        operators.catch(on_error),
        operators.repeat(),
    )

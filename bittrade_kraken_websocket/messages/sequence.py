from typing import Callable, Dict, List

import reactivex
from reactivex import Observable, compose, operators

from bittrade_kraken_websocket.development import debug_observer


class InvalidSequence(ValueError):
    pass
def correct_sequence_or_throw(x):
    previous, current = x
    if current[2]['sequence'] == previous[2]['sequence'] + 1:
        return current
    raise InvalidSequence()

def in_sequence() -> Callable[[Observable[List]], Observable[List]]:
    return compose(
        operators.start_with(["", "", {"sequence": 0}]),
        operators.pairwise(),
        operators.map(correct_sequence_or_throw),
    )


def repeat_on_invalid_sequence(do_this_first: Observable):
    """Retry on InvalidSequence error only
    This will allow to refresh the subscription
    """
    def on_error(exc, source):
        if type(exc) == InvalidSequence:
            return do_this_first
        return reactivex.throw(exc)

    return compose(
        operators.catch(on_error),
        operators.repeat()
    )
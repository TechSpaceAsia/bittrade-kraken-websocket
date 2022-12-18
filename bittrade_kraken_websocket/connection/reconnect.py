from typing import TypeVar

import reactivex
from reactivex import compose, operators, Observer, Observable
from reactivex.internal import infinite
from reactivex.operators import take, ignore_elements

very_first_retry = reactivex.interval(0.0)
first_retry = reactivex.interval(1.0)
next_retries = reactivex.interval(5.0)

_T = TypeVar("_T")


def retry_reconnect():
    def _retry_reconnect(source: Observable[_T]) -> Observable[_T]:
        current = [very_first_retry]
        gen = infinite()

        return reactivex.defer(
            lambda _: reactivex.concat_with_iterable(source for _ in gen)
        )

    return _retry_reconnect

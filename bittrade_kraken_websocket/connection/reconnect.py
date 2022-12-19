import time
from logging import getLogger
from typing import TypeVar, Optional
from collections.abc import Generator

import reactivex
from reactivex import Observable, operators
from reactivex.disposable import Disposable, CompositeDisposable
from reactivex.operators import take, ignore_elements, do_action, do
from bittrade_kraken_websocket.development import info_observer, debug_observer

_T = TypeVar("_T")

logger = getLogger(__name__)


def retry_with_backoff(stabilized: Observable=None, delays_pattern: Optional[Generator[float, None, None]]=None):
    """
    :param: stabilized: An observable that should emit after an amount of time (or a condition)
    When it successfully completes, the "delays" are reset to zero and follow the delays_pattern again
    This defaults to being active for 5 seconds without a completion
    :param: delays_pattern A generator which yields the waiting time during each failed iteration. A new iterator is created from the generator when the stabilized observable manages to complete
    Use an infinite generator for infinite repeats. Below are a few examples of backoff patterns
    ```
    # kraken's documentation suggested pattern
    def delays_pattern():
        yield 0.0
        yield 0.0
        yield 1.0
        while True:
            yield 5.0

    def exponential():
        yield 0.0
        value = 1.0
        while True:
            yield value
            value *= 2
    def finite():
        return iter([0.0, 3.0, 30.0])
    ```
    """
    if not stabilized:
        stabilized = reactivex.interval(5.0)
    if not delays_pattern:
        def gen():
            yield 0.0
            yield 0.0
            yield 1.0
            while True:
                yield 5.0
        delays_pattern = gen
    def _retry_reconnect(source: Observable[_T]) -> Observable[_T]:
        current_stable_subscription = [Disposable(action=lambda *_: logger.info('Cancelling fake initial sub'))]

        def delay_generator(scheduler):
            is_completed = False
            def complete():
                nonlocal is_completed
                is_completed = True

            while not is_completed:
                delay_by = next(delays[0])
                current_stable_subscription[0].dispose()
                logger.info('Back off delay is %s', delay_by)
                yield reactivex.interval(delay_by).pipe(
                    take(1),
                    ignore_elements()
                )
                if delay_by:
                    logger.info('Waited for back off; continuing')
                current_stable_subscription[0] = CompositeDisposable(
                    stabilized.pipe(
                        take(1),
                        do_action(on_completed=lambda: logger.info('Resetting delays'))
                    ).subscribe(on_completed=reset_delay),
                )
                yield source.pipe(
                    operators.do_action(on_completed=complete),
                    operators.catch(reactivex.empty(scheduler)),
                )


        delays = [delays_pattern()]


        def reset_delay(*args):
            logger.info('Delays have been reset')
            try:
                delays[0] = delays_pattern()
            except Exception as exc:
                logger.error('Failed to reset delays', exc)

        def deferred_action(scheduler):
            return reactivex.concat_with_iterable(
                obs for obs in delay_generator(scheduler)
            )

        return reactivex.defer(
            deferred_action
        )

    return _retry_reconnect

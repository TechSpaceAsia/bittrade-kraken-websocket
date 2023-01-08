from collections.abc import Generator
from logging import getLogger
from typing import Any, Callable, TypeVar, Optional, cast
from expression import curry_flip

import reactivex
from reactivex import Observable, operators
from reactivex.abc import DisposableBase, SchedulerBase
from reactivex.disposable import CompositeDisposable
from reactivex.operators import ignore_elements
from expression import Some, Nothing, Option

_T = TypeVar("_T")

logger = getLogger(__name__)


def kraken_patterns():
    yield 0.0
    yield 0.0
    yield 1.0
    while True:
        yield 5.0

@curry_flip(1)
def retry_with_backoff(
    source: Observable[_T],
    stabilized: Optional[Observable[Any]] = None,
    delays_pattern: Callable[[], Generator[float, None, None]] = kraken_patterns,
) -> Observable[_T]:
    """
    Reconnects to websocket with a backoff time.
    Note that when using this operator, the connection goes into a separate thread, you therefore need to keep the main thread alive

    :param: stabilized: An observable that should emit after an amount of time (or a condition)
            When it successfully completes, the "delays" are reset to zero and follow the delays_pattern again
            This defaults to "being active for 5 seconds without error"
    :param: delays_pattern: A generator which yields the waiting time during each failed iteration. A new iterator is created from the generator when the stabilized observable manages to complete
            Use an infinite generator for infinite repeats. Below are a few examples of backoff patterns

    Examples of delay generators:
    ### kraken's documentation suggested pattern
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
        stabilized = reactivex.timer(5.0)  # pragma: no cover

    # TODO move this to a SerialDisposable or something using switch_latest
    current_stable_subscription: Option[DisposableBase] = Nothing
    _is_first = True

    def delay_generator(scheduler: SchedulerBase):
        nonlocal _is_first, current_stable_subscription
        is_completed = False

        def complete():
            nonlocal is_completed
            is_completed = True

        while not is_completed:
            # TODO looks like we're not handling finite cases
            delay_by = next(delays[0])
            current_stable_subscription.bind(lambda x: Some(x.dispose()))
            if _is_first:
                _is_first = False
            else:
                logger.info("[BACKOFF] Back off delay is %s", delay_by)
            # Have to cheat a bit since there is no "Empty" observable type
            yield cast(Observable[_T], reactivex.timer(delay_by).pipe(ignore_elements()))
            if delay_by:
                logger.info("[BACKOFF] Waited for back off; continuing")
            current_stable_subscription = Some(CompositeDisposable(
                stabilized.subscribe(on_completed=reset_delay),
            ))
            yield source.pipe(
                operators.do_action(on_completed=complete),
                operators.catch(reactivex.empty(scheduler)),
            )

    delays = [delays_pattern()]

    def reset_delay(*_: Any):
        logger.info("[BACKOFF] Source stabilized; delays have been reset")
        try:
            delays[0] = delays_pattern()
        except Exception as exc:
            logger.error("[BACKOFF] Failed to reset delays", exc)

    def deferred_action(scheduler: SchedulerBase):
        return reactivex.concat_with_iterable(
            obs for obs in delay_generator(scheduler)
        )

    return reactivex.defer(deferred_action)

__all__ = [
    "retry_with_backoff",
    "kraken_patterns",
]
from typing import TypeVar

import reactivex
from reactivex import compose, operators, Observer, Observable
from reactivex.disposable import Disposable
from reactivex.internal import infinite
from reactivex.operators import take, ignore_elements, do_action

_T = TypeVar("_T")

def retry_reconnect(stabilized: Observable, delays_pattern=None):
    delays_pattern = delays_pattern or [0.0, 0.0, 1.0, 5.0]
    def _retry_reconnect(source: Observable[_T]) -> Observable[_T]:
        current_stable_subscription = [Disposable()]
        gen = infinite()

        # def delay_generator():
        #     delay_by = delays[0].pop(0) if len(delays[0]) else delays_pattern[-1]
        #     yield reactivex.interval(delay_by)
        #     yield source


        delays = [list(delays_pattern)]
        current_delay = [delays[0].pop(0)]

        def reset_delay(scheduler):
            current_delay[0] = 0.0
            delays[0] = list(delays_pattern)


        def increment_delay(scheduler):
            if len(delays[0]):
                current_delay[0] = delays[0].pop(0)
            # Unless stopped by another completion, this will reset the delay once we feel its stabilized
            current_stable_subscription[0] = stabilized.pipe(
                operators.delay_subscription(current_delay[0], scheduler=scheduler),
            ).subscribe(on_completed=lambda: reset_delay(scheduler))

        def stop_reset():
            current_stable_subscription[0].dispose()

        def deferred_action(scheduler):
            return reactivex.concat_with_iterable(
                source.pipe(
                    do_action(on_completed=stop_reset),
                    operators.delay_subscription(current_delay[0], scheduler=scheduler),
                    do_action(on_completed=lambda: increment_delay(scheduler)),
                ) for _ in gen
            )


        return reactivex.defer(
            deferred_action
        )

    return _retry_reconnect

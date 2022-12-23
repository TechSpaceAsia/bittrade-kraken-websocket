from typing import Optional

from reactivex import Observable
from reactivex.abc import ObserverBase, DisposableBase
from reactivex.testing import ReactiveTest, TestScheduler

from bittrade_kraken_websocket.connection.reconnect import retry_with_backoff
from tests.helpers.subscriptions import from_to

on_next = ReactiveTest.on_next
on_error = ReactiveTest.on_error
on_completed = ReactiveTest.on_completed
subscribe = ReactiveTest.subscribe


def test_reconnect_completes_after_stabilized():
    scheduler = TestScheduler()
    source = scheduler.create_cold_observable(
        on_next(3, 42),
        on_next(4, 43),
        on_error(10, Exception('HMM'))
    )
    stabilized = scheduler.create_cold_observable(on_completed(5))

    def create():
        return source.pipe(
            retry_with_backoff(stabilized)
        )

    results = scheduler.start(create, created=1, subscribed=2, disposed=25)

    assert results.messages == [
        on_next(5, 42),
        on_next(6, 43),
        on_next(15, 42),
        on_next(16, 43)
    ]


def test_reconnect_completes_before_stabilized():
    scheduler = TestScheduler()
    source = scheduler.create_cold_observable(
        on_next(3, 'A'),
        on_next(4, 'B'),
        on_error(10, Exception())
    )
    stabilized = scheduler.create_cold_observable(on_completed(12))

    def create():
        return source.pipe(
            retry_with_backoff(stabilized)
        )

    results = scheduler.start(create, created=1, subscribed=2, disposed=75)

    assert results.messages == [
        on_next(5, 'A'),
        on_next(6, 'B'),
        # first iteration stops at 2+10
        # second starts at 12
        on_next(15, 'A'),  # immediate resubscribe
        on_next(16, 'B'),
        # second iteration stops at 22
        # 3rd starts at 23
        on_next(26, 'A'),  # one tick delay
        on_next(27, 'B'),
        # 3rd stops at 33
        # fourth starts at 38
        on_next(41, 'A'),  # 5 ticks delay
        on_next(42, 'B'),
        # fourth stops at 48
        # 5th starts at 53
        on_next(56, 'A'),  # still 5 ticks delay
        on_next(57, 'B'),
        # 5h stops at 63
        # 6th starts at 68
        on_next(71, 'A'),  # still 5 ticks delay
        on_next(72, 'B'),
    ]
    assert source.subscriptions == [
        from_to(2, 12),
        from_to(12, 22),
        from_to(23, 33),
        from_to(38, 48),
        from_to(53, 63),
        from_to(68, 75),
    ]


def test_reconnect_complex_case():
    scheduler = TestScheduler()
    sources = [
        # First one will complete early
        scheduler.create_cold_observable(
            on_next(3, 'A'),
            on_error(10, Exception())
        ),
        # Second one too will complete early
        scheduler.create_cold_observable(
            on_next(5, 'A'),
            on_error(5, Exception())
        ),
        # Third is stable again but does not emit?
        scheduler.create_cold_observable(
            on_error(15, Exception())
        ),
        # Fourth early
        scheduler.create_cold_observable(
            on_next(7, 'A'),
            on_error(11, Exception())
        ),
        # 5th early
        scheduler.create_cold_observable(
            on_error(5, Exception())
        ),
        # 6th early
        scheduler.create_cold_observable(
            on_next(6, 'AAA'),
            on_error(8, Exception())
        ),
        scheduler.create_cold_observable(
            on_completed(2)
        )
    ]

    stabilized = scheduler.create_cold_observable(on_completed(12))

    def factory(observer: ObserverBase, scheduler_: Optional[DisposableBase] = None):
        return sources.pop(0).subscribe(observer, scheduler=scheduler_)
    source = Observable(factory)

    def create():
        return source.pipe(
            retry_with_backoff(stabilized)
        )

    results = scheduler.start(create, created=1, subscribed=2, disposed=75)

    assert results.messages == [
        on_next(5, 'A'),
        # first iteration stops at 2+10
        # second starts at 12
        on_next(17, 'A'),  # immediate resubscribe
        # second iteration stops at 17
        # 3rd starts at 18
        # At 30, triggers "stable"
        # 3rd stops at 33
        # fourth starts at 33 because delay has been reset
        on_next(40, 'A'),
        # fourth stops at 44
        # 5th starts at 44
        # 5h stops at 49
        # 6th starts at 50
        on_next(56, 'AAA'),
        # stops at 58
        # completes if underlying one completes?
        on_completed(65)
    ]

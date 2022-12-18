import pytest
import reactivex
from reactivex import operators, Observable
from reactivex.notification import OnError
from reactivex.testing import ReactiveTest, TestScheduler
from reactivex.testing.subscription import Subscription

from bittrade_kraken_websocket.connection.reconnect import retry_reconnect
from bittrade_kraken_websocket.events.events import EVENT_SUBSCRIBE
from bittrade_kraken_websocket.events.request_response import request_response, _response_ok, RequestResponseError
from tests.helpers.from_sample import from_sample
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
        on_completed(10)
    )
    stabilized = scheduler.create_cold_observable(on_completed(5.0))
    def create():
        return source.pipe(
            retry_reconnect(stabilized)
        )

    results = scheduler.start(create, created=1, subscribed=2, disposed=25)

    assert results.messages == [
        on_next(5.0, 42),
        on_next(6.0, 43),
        on_next(15.0, 42),
        on_next(16.0, 43)
    ]

def test_reconnect_completes_before_stabilized():
    scheduler = TestScheduler()
    source = scheduler.create_cold_observable(
        on_next(3, 'A'),
        on_next(4, 'B'),
        on_completed(10)
    )
    stabilized = scheduler.create_cold_observable(on_completed(12.0))
    def create():
        return source.pipe(
            retry_reconnect(stabilized)
        )

    results = scheduler.start(create, created=1, subscribed=2, disposed=75)

    assert results.messages == [
        on_next(5.0, 'A'),
        on_next(6.0, 'B'),
        # first iteration stops at 2+10
        # second starts at 12
        on_next(15.0, 'A'),  # immediate resubscribe
        on_next(16.0, 'B'),
        # second iteration stops at 22
        # 3rd starts at 23
        on_next(26.0, 'A'),  # one tick delay
        on_next(27.0, 'B'),
        # 3rd stops at 33
        # fourth starts at 38
        on_next(41.0, 'A'),  # 5 ticks delay
        on_next(42.0, 'B'),
        # fourth stops at 48
        # 5th starts at 53
        on_next(56.0, 'A'),  # still 5 ticks delay
        on_next(57.0, 'B'),
        # 5h stops at 63
        # 6th starts at 68
        on_next(71.0, 'A'),  # still 5 ticks delay
        on_next(72.0, 'B'),
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
            on_completed(10)
        ),
        # Second one too will complete early
        scheduler.create_cold_observable(
            on_next(5, 'A'),
            on_completed(5)
        ),
        # Third is stable again but does not emit?
        scheduler.create_cold_observable(
            on_completed(15)
        ),
        # Fourth early
        scheduler.create_cold_observable(
            on_next(7, 'A'),
            on_completed(11)
        ),
        # 5th early
        scheduler.create_cold_observable(
            on_completed(5)
        ),
        # 6th early
        scheduler.create_cold_observable(
            on_next(6, 'AAA'),
            on_completed(8)
        )
    ]

    stabilized = scheduler.create_cold_observable(on_completed(12.0))
    def factory(observer, scheduler=scheduler):
        return sources.pop(0).subscribe(observer, scheduler=scheduler)
    source = Observable(factory)
    def create():
        return source.pipe(
            retry_reconnect(stabilized)
        )

    results = scheduler.start(create, created=1, subscribed=2, disposed=75)

    assert results.messages == [
        on_next(5.0, 'A'),
        # first iteration stops at 2+10
        # second starts at 12
        on_next(17.0, 'A'),  # immediate resubscribe
        # second iteration stops at 17
        # 3rd starts at 18
        # At 30, triggers "stable"
        # 3rd stops at 33
        # fourth starts at 33 because delay has been reset
        on_next(40.0, 'A'),
        # fourth stops at 44
        # 5th starts at 44
        # 5h stops at 49
        # 6th starts at 50
        on_next(56.0, 'AAA'),
        # stops at 58
        # tries to connect and errors at 63
        on_error(63, TypeError('pop from empty list'))
    ]
    


import pytest
from reactivex import operators
from reactivex.notification import OnError
from reactivex.testing import ReactiveTest, TestScheduler
from reactivex.testing.subscription import Subscription

from bittrade_kraken_websocket.connection.reconnect import retry_reconnect
from bittrade_kraken_websocket.events.events import EVENT_SUBSCRIBE
from bittrade_kraken_websocket.events.request_response import request_response, _response_ok, RequestResponseError
from tests.helpers.from_sample import from_sample

on_next = ReactiveTest.on_next
on_error = ReactiveTest.on_error
on_completed = ReactiveTest.on_completed
subscribe = ReactiveTest.subscribe


def test_reconnect():
    scheduler = TestScheduler()
    source = scheduler.create_cold_observable(
        on_next(3, 42),
        on_next(4, 43),
        on_completed(10)
    )

    def create():
        return source.pipe(
            retry_reconnect()
        )

    results = scheduler.start(create, created=1, subscribed=2)

    assert results.messages == [
        on_next(210, 42),
        on_next(220, 43),
        on_next(240, 42),
        on_next(250, 43),
    ]
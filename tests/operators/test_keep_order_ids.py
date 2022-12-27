from reactivex.notification import OnError
from reactivex.testing import ReactiveTest, TestScheduler
from bittrade_kraken_websocket.operators import reduce_order_ids
from tests.helpers.from_sample import from_sample

on_next = ReactiveTest.on_next
on_error = ReactiveTest.on_error
subscribe = ReactiveTest.subscribe


def test_keep_order_ids():
    scheduler = TestScheduler()
    observable = scheduler.create_hot_observable(
        from_sample('openOrders.json')
    )

    def create():
        return observable.pipe(reduce_order_ids())

    results = scheduler.start(create)
    # Due to mutability, not testing actual values
    assert len(results.messages) == 2
    assert results.messages[0].time == 210
    assert results.messages[1].time == 240



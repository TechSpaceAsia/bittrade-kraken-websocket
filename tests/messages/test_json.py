from reactivex.testing import ReactiveTest, TestScheduler

from bittrade_kraken_websocket.messages.json import to_json

on_next = ReactiveTest.on_next
on_error = ReactiveTest.on_error
subscribe = ReactiveTest.subscribe


def test_to_json():
    scheduler = TestScheduler()
    observable = scheduler.create_hot_observable(
        on_next(300, 'not json is ignored'),
        on_next(500, '[42, "arrays work"]'),
        on_next(600, '{"also": "dicts"}'),
    )

    def create():
        return observable.pipe(to_json())

    results = scheduler.start(create)
    assert results.messages == [
        on_next(500, [42, "arrays work"]),
        on_next(600, {"also": "dicts"}),
    ]

from reactivex.testing import ReactiveTest, TestScheduler

from bittrade_kraken_websocket.messages.sequence import in_sequence, InvalidSequence, retry_on_invalid_sequence

on_next = ReactiveTest.on_next
on_error = ReactiveTest.on_error
on_completed = ReactiveTest.on_completed
subscribe = ReactiveTest.subscribe


def test_in_sequence_accepts_any_first_value():
    scheduler = TestScheduler()
    source = scheduler.create_hot_observable(
        on_next(250, ["", "", {"sequence": 2}]),
        on_next(350, ["", "", {"sequence": 4}]),
    )

    results = scheduler.start(lambda: source.pipe(in_sequence()))

    assert results.messages == [
        on_next(250, ["", "", {"sequence": 2}]),
        on_error(350, InvalidSequence())
    ]


def test_in_sequence_ok_until_completion():
    scheduler = TestScheduler()
    source = scheduler.create_hot_observable(
        on_next(250, ["w", "", {"sequence": 1}]),
        on_next(350, ["", "x", {"sequence": 2}]),
        on_next(440, ["", "", {"sequence": 3}]),
        on_next(500, ["", "", {"sequence": 4}]),
        on_completed(510),
    )

    results = scheduler.start(lambda: source.pipe(in_sequence()))

    assert results.messages == [
        on_next(250, ["w", "", {"sequence": 1}]),
        on_next(350, ["", "x", {"sequence": 2}]),
        on_next(440, ["", "", {"sequence": 3}]),
        on_next(500, ["", "", {"sequence": 4}]),
        on_completed(510),
    ]


def test_in_ok_until_error():
    scheduler = TestScheduler()
    source = scheduler.create_hot_observable(
        on_next(250, ["a", "b", {"sequence": 1}]),
        on_next(350, ["a", "b", {"sequence": 2}]),
        on_next(440, ["a", "b", {"sequence": 3}]),
        on_next(500, ["a", "b", {"sequence": 4}]),
        on_error(510, Exception('abc')),
    )

    results = scheduler.start(lambda: source.pipe(in_sequence()))

    assert results.messages == [
        on_next(250, ["a", "b", {"sequence": 1}]),
        on_next(350, ["a", "b", {"sequence": 2}]),
        on_next(440, ["a", "b", {"sequence": 3}]),
        on_next(500, ["a", "b", {"sequence": 4}]),
        on_error(510, Exception('abc')),
    ]


def test_in_fail_halfway():
    scheduler = TestScheduler()
    source = scheduler.create_hot_observable(
        on_next(210, ["", "", {"sequence": 1}]),
        on_next(220, ["", "", {"sequence": 2}]),
        on_next(240, ["", "", {"sequence": 4}]),
        on_next(250, ["", "", {"sequence": 3}]),
    )

    results = scheduler.start(lambda: source.pipe(in_sequence()))

    assert results.messages == [
        on_next(210, ["", "", {"sequence": 1}]),
        on_next(220, ["", "", {"sequence": 2}]),
        on_error(240, InvalidSequence()),
    ]


def test_retry_on_invalid_sequence():
    scheduler = TestScheduler()
    source = scheduler.create_cold_observable(
        on_next(100, {"sequence": 1}),
        on_next(120, {"sequence": 2}),
        on_error(140, InvalidSequence()),
    )
    results = scheduler.start(lambda: source.pipe(
        retry_on_invalid_sequence()
    ), created=10, subscribed=70, disposed=540)

    assert results.messages == [
        on_next(70+100, {"sequence": 1}),
        on_next(70+120, {"sequence": 2}),
        # at 70+140 catches exception
        on_next(70+140+100, {"sequence": 1}),
        on_next(70+140+120, {"sequence": 2}),
        on_next(450, {"sequence": 1}),
        on_next(470, {"sequence": 2}),
    ]


def test_retry_on_invalid_sequence_combined_with_in_sequence():
    scheduler = TestScheduler()
    source = scheduler.create_hot_observable(
        on_next(50, ["", "", {"sequence": 5}]),
        on_next(100, ["", "", {"sequence": 1}]),
        on_next(120, ["", "", {"sequence": 2}]),
        on_next(140, ["", "", {"sequence": 5}]),
        on_next(200, ["", "", {"sequence": 1}]),
        on_next(250, ["", "", {"sequence": 2}]),
        on_next(300, ["", "", {"sequence": 5}]),
        on_next(400, ["", "", {"sequence": 1}]),
        on_next(500, ["", "", {"sequence": 2}]),
        on_next(600, ["", "", {"sequence": 5}]),
    )
    results = scheduler.start(lambda: source.pipe(
        in_sequence(),
        retry_on_invalid_sequence()
    ), created=10, subscribed=70, disposed=540)

    assert results.messages == [
        on_next(100, ["", "", {"sequence": 1}]),
        on_next(120, ["", "", {"sequence": 2}]),
        on_next(200, ["", "", {"sequence": 1}]),
        on_next(250, ["", "", {"sequence": 2}]),
        on_next(400, ["", "", {"sequence": 1}]),
        on_next(500, ["", "", {"sequence": 2}]),
    ]

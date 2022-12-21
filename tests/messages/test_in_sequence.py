from reactivex.testing import ReactiveTest, TestScheduler

from bittrade_kraken_websocket.messages.sequence import in_sequence, InvalidSequence, repeat_on_invalid_sequence

on_next = ReactiveTest.on_next
on_error = ReactiveTest.on_error
on_completed = ReactiveTest.on_completed
subscribe = ReactiveTest.subscribe


def test_in_sequence_instant_fail():
    scheduler = TestScheduler()
    source = scheduler.create_hot_observable(
        on_next(250, {"sequence": 2}),
        on_next(350, {"sequence": 3})
    )

    results = scheduler.start(lambda: source.pipe(in_sequence()))

    assert results.messages == [
        on_error(250, InvalidSequence())
    ]


def test_in_ok_until_completion():
    scheduler = TestScheduler()
    source = scheduler.create_hot_observable(
        on_next(250, {"sequence": 1}),
        on_next(350, {"sequence": 2}),
        on_next(440, {"sequence": 3}),
        on_next(500, {"sequence": 4}),
        on_completed(510),
    )

    results = scheduler.start(lambda: source.pipe(in_sequence()))

    assert results.messages == [
        on_next(250, {"sequence": 1}),
        on_next(350, {"sequence": 2}),
        on_next(440, {"sequence": 3}),
        on_next(500, {"sequence": 4}),
        on_completed(510),
    ]


def test_in_ok_until_error():
    scheduler = TestScheduler()
    source = scheduler.create_hot_observable(
        on_next(250, {"sequence": 1}),
        on_next(350, {"sequence": 2}),
        on_next(440, {"sequence": 3}),
        on_next(500, {"sequence": 4}),
        on_error(510, Exception('abc')),
    )

    results = scheduler.start(lambda: source.pipe(in_sequence()))

    assert results.messages == [
        on_next(250, {"sequence": 1}),
        on_next(350, {"sequence": 2}),
        on_next(440, {"sequence": 3}),
        on_next(500, {"sequence": 4}),
        on_error(510, Exception('abc')),
    ]


def test_in_fail_halfway():
    scheduler = TestScheduler()
    source = scheduler.create_hot_observable(
        on_next(210, {"sequence": 1}),
        on_next(220, {"sequence": 2}),
        on_next(240, {"sequence": 4}),
        on_next(250, {"sequence": 3}),
    )

    results = scheduler.start(lambda: source.pipe(in_sequence()))

    assert results.messages == [
        on_next(210, {"sequence": 1}),
        on_next(220, {"sequence": 2}),
        on_error(240, InvalidSequence()),
    ]


def test_repeat_on_invalid_sequence():
    scheduler = TestScheduler()
    source = scheduler.create_cold_observable(
        on_next(100, {"sequence": 1}),
        on_next(120, {"sequence": 2}),
        on_error(140, InvalidSequence()),
    )
    do_this_first = scheduler.create_cold_observable(
        on_next(20, 'Hi'),
        on_completed(30)
    )
    results = scheduler.start(lambda: source.pipe(
        repeat_on_invalid_sequence(do_this_first)
    ), created=10, subscribed=70, disposed=540)

    assert results.messages == [
        on_next(70+100, {"sequence": 1}),
        on_next(70+120, {"sequence": 2}),
        # at 70+140 catches exception
        on_next(70+140+20, 'Hi'),
        # at 70+140+30 done with "do this first, starts again
        on_next(70+140+30+100, {"sequence": 1}),
        on_next(70+140+30+120, {"sequence": 2}),
        on_next(70+140+30+140+20, 'Hi'),
        on_next(510, {"sequence": 1}),
        on_next(530, {"sequence": 2}),
    ]

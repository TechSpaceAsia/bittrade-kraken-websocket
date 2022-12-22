import reactivex
from reactivex.disposable import CompositeDisposable
from reactivex.testing import ReactiveTest, TestScheduler
from reactivex import operators, Observable, Observer, scheduler
import time
import threading

from bittrade_kraken_websocket.development import debug_observer, info_observer

on_next = ReactiveTest.on_next
on_error = ReactiveTest.on_error
on_completed = ReactiveTest.on_completed
subscribe = ReactiveTest.subscribe


def test_double():
    # Create a scheduler
    scheduler = TestScheduler()
    # Define source
    source = scheduler.create_hot_observable(
        on_next(250, 3),
        on_next(350, 5),
    )

    # Define how the observable/operator is used on the source
    def create():
        print(scheduler.clock)
        return source.pipe(operators.map(lambda x: 2 * x))

    # trigger subscription and record emissions
    results = scheduler.start(create)

    # check the messages and potentially subscriptions
    assert results.messages == [
        on_next(250, 6),
        on_next(350, 10),
    ]


# Code to test; takes a sequence of integers and passes through,
# unless they are not in sequence in which case it errors
def in_sequence_or_throw():
    return reactivex.compose(
        operators.start_with(None),
        operators.pairwise(),
        operators.flat_map(lambda x: reactivex.of(x[1]) if (
                x[0] is None or x[1] == x[0] + 1
        ) else reactivex.throw(ValueError('Sequence error')))
    )


def test_in_sequence_or_throw_error():
    scheduler = TestScheduler()
    source = reactivex.from_marbles('--1-4-3-', timespan=50, scheduler=scheduler)
    result = scheduler.start(lambda: source.pipe(
        in_sequence_or_throw(),
    ), created=1, subscribed=30)

    assert result.messages == [
        on_next(130, 1),
        on_error(230, ValueError('Sequence error'))
    ]
    # Often it's better not to test the exact exception; we can test a specific emit as follows:
    message, err = result.messages
    assert message.time == 130
    assert err.time == 230
    assert message.value.kind == 'N'  # Notification
    assert err.value.kind == 'E'  # E for errors
    assert message.value.value == 1
    assert type(err.value.exception) == ValueError  # look at .exception for errors


def test_operator():
    scheduler = TestScheduler()
    # Create source
    source = scheduler.create_cold_observable(
        on_next(300, 1), on_next(400, 2), on_next(500, 3), on_completed(600)
    )
    # Here is another way to create the same observable for the test,
    # as long as we set the correct scheduler
    source = reactivex.from_marbles('------1-2-3-|', timespan=50, scheduler=scheduler)
    # You can shorten the "create" function as above to a lambda with no arguments
    result = scheduler.start(lambda: source.pipe(
        in_sequence_or_throw(),
    ))
    assert result.messages == [
        on_next(500, 1), on_next(600, 2), on_next(700, 3), on_completed(800)
    ]


def test_my_observable_factory():
    from reactivex.disposable import Disposable, CompositeDisposable
    a = 42

    def factory(observer: Observer, scheduler=None):
        def increment():
            nonlocal a
            a += 1

        sub = Disposable(action=increment)
        return CompositeDisposable(
            sub,
            reactivex.timer(20, scheduler=scheduler).subscribe(observer)
        )

    scheduler = TestScheduler()
    result = scheduler.start(lambda: Observable(factory))
    assert result.messages == [
        on_next(220, 0),
        on_completed(220)
    ]
    assert a == 43


def test_multiple():
    scheduler = TestScheduler()
    source = reactivex.from_marbles('-1-4-3-|', timespan=50, scheduler=scheduler)
    odd, even = source.pipe(
        operators.partition(lambda x: x % 2),
    )
    steven = scheduler.create_observer()
    todd = scheduler.create_observer()

    even.subscribe(steven)
    odd.subscribe(todd)

    # Note! Since it's not "start" which creates the subscription, they actually occur at t=0
    scheduler.start()

    assert steven.messages == [
        on_next(150, 4),
        on_completed(350)
    ]
    assert todd.messages == [
        on_next(50, 1),
        on_next(250, 3),
        on_completed(350)
    ]


from reactivex.testing.subscription import Subscription
def test_subscriptions():
    scheduler = TestScheduler()
    source = scheduler.create_cold_observable()  # "infinite"
    subs = []
    shared = source.pipe(
        operators.share()
    )
    """first sub"""
    scheduler.schedule_relative(200, lambda *_: subs.append(shared.subscribe(scheduler=scheduler)))
    # second sub, should not sub to source itself
    scheduler.schedule_relative(300, lambda *_: subs.append(shared.subscribe(scheduler=scheduler)))
    scheduler.schedule_relative(500, lambda *_: subs[1].dispose())
    scheduler.schedule_relative(600, lambda *_: subs[0].dispose())
    """end first sub"""
    # no existing sub should sub again onto source - we never dispose of it
    scheduler.schedule_relative(900, lambda *_: subs.append(shared.subscribe(scheduler=scheduler)))

    scheduler.start()
    # Check that the submissions on the source are as expected
    assert source.subscriptions == [
        Subscription(200, 600),
        Subscription(900),  # represents an infinite subscription
    ]


def test_hot():
    scheduler = TestScheduler()
    # hot starts at 0 but sub starts at 200 so we'll miss 190
    source = scheduler.create_hot_observable(
        on_next(190, 5),
        on_next(300, 42),
        on_completed(500)
    )
    result = scheduler.start(lambda: source.pipe(
        operators.to_marbles(timespan=20, scheduler=scheduler)
    ))

    message = result.messages[0]
    # sub starts at 200 and we emit at 300 - since this is a hot observable, aka 5 ticks of 20 (timespan=20 in to_marbles)
    # then we get the 42 emit and then blank until 500, so 10 ticks*20
    assert message.value.value == '-----(42)----------|'



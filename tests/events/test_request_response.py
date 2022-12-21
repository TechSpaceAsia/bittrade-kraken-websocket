from typing import Dict
from unittest.mock import MagicMock

import pytest
import reactivex
from reactivex import operators
from reactivex.testing import ReactiveTest, TestScheduler
from reactivex.testing.subscription import Subscription

from bittrade_kraken_websocket.events.request_response import request_response, _response_ok, RequestResponseError, \
    wait_for_response, build_matcher
from bittrade_kraken_websocket.events.subscribe import request_response_factory
from tests.helpers.from_sample import from_sample

on_next = ReactiveTest.on_next
on_error = ReactiveTest.on_error
on_completed = ReactiveTest.on_completed
subscribe = ReactiveTest.subscribe



def test_subscribe_v2():
    scheduler = TestScheduler()
    inner_obs = scheduler.create_cold_observable(
        on_next(10, 'abc'),
        on_next(40, 'd'),
    )
    inner_obs2 = scheduler.create_cold_observable(
        on_next(30, 'AAA'),
        on_next(300, 42),
    )
    messages = scheduler.create_hot_observable(
        on_next(300, inner_obs),
        on_next(400, inner_obs2),
        on_next(500, inner_obs)
    )
    result = scheduler.start(
        lambda: messages.pipe(
            operators.switch_latest()
        )
    )
    assert result.messages == [
        on_next(310, 'abc'),
        on_next(340, 'd'),
        on_next(430, 'AAA'),
        on_next(510, 'abc'),
        on_next(540, 'd'),
    ]
    assert inner_obs.subscriptions == [Subscription(300, 400), Subscription(500, 1000)]
    assert inner_obs2.subscriptions == [Subscription(400, 500)]

def test_request_response_factory_timeout_using_next():
    scheduler = TestScheduler()
    messages = scheduler.create_hot_observable(
        on_next(300, {"event": "addOrderStatus", "reqid": 9}),
        on_next(400, {"event": "statusNotMatter", "reqid": 100}),
        on_next(600, {"event": "statusNotMatter", "reqid": 300}), # 600 > 200+300 - too late
    )
    timeout = scheduler.create_cold_observable(on_next(300, None))
    on_enter = MagicMock()
    result = scheduler.start(lambda: request_response_factory(on_enter=on_enter, message_id=300, messages=messages, timeout=timeout))
    assert result.messages == [on_error(500, TimeoutError())]

    assert messages.subscriptions == [Subscription(200, 500)]
    assert timeout.subscriptions == [Subscription(200, 500)]

    on_enter.assert_called_once_with()



def test_request_response_timeout_using_next():
    scheduler = TestScheduler()
    sender = scheduler.create_observer()
    messages = scheduler.create_hot_observable(
        on_next(300, {"event": "addOrderStatus", "reqid": 9}),
        on_next(400, {"event": "incorrectStatus", "reqid": 100}),
    )
    timeout = scheduler.create_hot_observable(on_next(800, None))
    factory = request_response(sender, messages, timeout)

    def create():
        return factory({"abc": 42, "reqid": 300})

    result = scheduler.start(create)
    assert len(result.messages) == 1
    assert result.messages[0].time == 800
    assert type(result.messages[0].value.exception) == TimeoutError

    assert messages.subscriptions == [Subscription(200, 800)]
    assert timeout.subscriptions == [Subscription(200, 800)]


def test_request_response_timeout_using_error():
    scheduler = TestScheduler()
    sender = scheduler.create_observer()
    messages = scheduler.create_hot_observable(
        on_next(300, {"event": "addOrderStatus", "reqid": 9}),
        on_next(600, {"event": "editOrderStatus", "reqid": 100}),
    )
    timeout = scheduler.create_hot_observable(on_error(400, TimeoutError('abc')))
    factory = request_response(sender, messages, timeout)

    def create():
        return factory({"abc": 42, "reqid": 100})

    result = scheduler.start(create)
    assert result.messages == [on_error(400, TimeoutError('abc'))]
    assert messages.subscriptions == [Subscription(200, 400)]
    assert timeout.subscriptions == [Subscription(200, 400)]


def test_request_response_success():
    scheduler = TestScheduler()
    sender = scheduler.create_observer()
    messages = scheduler.create_hot_observable(
        on_next(300, {"event": "addOrderStatus", "reqid": 9}),
        on_next(500, {"event": "addOrderStatus", "reqid": 100, "more": "stuff"}),
        on_next(600, {"event": "addOrderStatus", "reqid": 100, "more": "stuff again"}),
    )
    timeout = scheduler.create_hot_observable(on_next(800, None))
    factory = request_response(sender, messages, timeout)

    def create():
        return factory({"abc": 42, "reqid": 100})

    result = scheduler.start(create)
    assert result.messages == [
        on_next(500, {"event": "addOrderStatus", "reqid": 100, "more": "stuff"}),
        on_completed(500)
    ]

    assert messages.subscriptions == [Subscription(200, 500)]
    assert timeout.subscriptions == [Subscription(200, 500)]


def test_request_response_success():
    scheduler = TestScheduler()
    sender = scheduler.create_observer()
    messages = scheduler.create_hot_observable(
        on_next(300, {"event": "addOrderStatus", "reqid": 9}),
        on_next(500, {"event": "addOrderStatus", "reqid": 100, "more": "stuff"}),
        on_next(600, {"event": "addOrderStatus", "reqid": 100, "more": "stuff again"}),
    )
    timeout = scheduler.create_cold_observable(on_next(800, None))
    factory = request_response(sender, messages, timeout)

    def create():
        return factory({"abc": 42, "reqid": 100})

    result = scheduler.start(create)
    assert result.messages == [
        on_next(500, {"event": "addOrderStatus", "reqid": 100, "more": "stuff"}),
        on_completed(500)
    ]

    assert messages.subscriptions == [Subscription(200, 500)]
    assert timeout.subscriptions == [Subscription(200, 500)]


def test_request_response_success_repeat_same_id():
    scheduler = TestScheduler()
    sender = scheduler.create_observer()
    messages = scheduler.create_hot_observable(
        on_next(300, {"event": "addOrderStatus", "reqid": 9}),
        on_next(500, {"event": "addOrderStatus", "reqid": 100, "more": "stuff"}),
        on_next(600, {"event": "addOrderStatus", "reqid": 100, "more": "stuff again"}),
        on_next(700, {"event": "addOrderStatus", "reqid": 9}),
        on_next(900, {"event": "addOrderStatus", "reqid": 100, "more": "LALA"}),
        on_next(1000, {"event": "addOrderStatus", "reqid": 100, "more": "stuff again"}),
    )
    # Important! This confirms that the timeout is "per request" since we are still live at t=900. As long as the timeout isn't exceeded between t=0-500 and t=680-900
    timeout = scheduler.create_cold_observable(on_next(800, None))
    factory = request_response(sender, messages, timeout)

    o1 = factory({"abc": 42, "reqid": 100})
    o2 = factory({"abc": 43, "reqid": 100})
    v = scheduler.create_observer()
    scheduler.schedule_absolute(680, lambda *_: o2.subscribe(v))
    o1.subscribe(v)  # unlike when using `start` this sub starts at 0, not 200
    scheduler.start()
    assert v.messages == [
        on_next(500, {"event": "addOrderStatus", "reqid": 100, "more": "stuff"}),
        on_completed(500),
        on_next(900, {"event": "addOrderStatus", "reqid": 100, "more": "LALA"}),
        on_completed(900),
    ]

    assert messages.subscriptions == [Subscription(0, 500), Subscription(680, 900)]
    assert timeout.subscriptions == messages.subscriptions


def test_response_ok():
    assert _response_ok({"status": "ok", "lala": "blop"}) == {"status": "ok", "lala": "blop"}
    with pytest.raises(RequestResponseError) as exc:
        _response_ok({"status": "error", "errorMessage": "lala"})
    assert str(exc.value) == 'lala'
    with pytest.raises(Exception) as exc:
        _response_ok({"other stuff": "error", "errorMessage": "lala"})


def test_response_ok_other_status():
    assert _response_ok({"status": "subscribed", "lala": "blop"}, good_status="subscribed") == {"status": "subscribed",
                                                                                                "lala": "blop"}
    with pytest.raises(RequestResponseError) as exc:
        _response_ok({"status": "bad one", "errorMessage": "lala"}, bad_status="bad one")
    with pytest.raises(Exception) as exc:
        _response_ok({"other stuff": "error", "errorMessage": "lala", "status": "other"}, "good", "bad")


def test_request_response_success_reuse_different_ids():
    scheduler = TestScheduler()
    sender = scheduler.create_observer()
    messages = scheduler.create_hot_observable(
        on_next(300, {"event": "addOrderStatus", "reqid": 100, "more": "stuff"}),
        on_next(600, {"event": "addOrderStatus", "reqid": 100, "more": "stuff again"}),
        on_next(700, {"event": "addOrderStatus", "reqid": 9}),
        on_next(800, {"event": "addOrderStatus", "reqid": 5, "more": "LADIDA"}),
        on_next(900, {"event": "addOrderStatus", "reqid": 100, "more": "stuff again"}),
    )
    # This confirms that the timeout is "per request" since we are still live at t=900. As long as the timeout isn't exceeded between t=0-500 and t=680-900
    timeout = scheduler.create_cold_observable(on_next(400, None))

    factory = request_response(sender, messages, timeout)

    o1 = factory({"abc": 42, "reqid": 100})
    o2 = factory({"abc": 43, "reqid": 5})
    v = scheduler.create_observer()
    scheduler.schedule_absolute(650, lambda *_: o2.subscribe(v))
    o1.subscribe(v)  # unlike when using `start` this sub starts at 0, not 200
    scheduler.start()
    assert v.messages == [
        on_next(300, {"event": "addOrderStatus", "reqid": 100, "more": "stuff"}),
        on_completed(300),
        on_next(800, {"event": "addOrderStatus", "reqid": 5, "more": "LADIDA"}),
        on_completed(800),
    ]

    assert messages.subscriptions == [Subscription(0.0, 300.0), Subscription(650.0, 800)]
    assert timeout.subscriptions == messages.subscriptions


def test_request_response_subscription():
    scheduler = TestScheduler()
    sender = scheduler.create_observer()
    messages = scheduler.create_hot_observable(
        from_sample('subscribe.jsonl')
    )
    timeout = scheduler.create_cold_observable(on_next(400, None))
    caller = request_response(
        sender, messages, timeout
    )
    results = scheduler.start(
        lambda: caller({"reqid": 12870497960778414923, 'event': 'subscribe', 'pair': ['USDT/USD'], 'subscription': {'name': 'ticker'}}))

    assert results.messages == [
        on_next(220, {"channelID": 1028, "reqid": 12870497960778414923, "channelName": "ticker", "event": "subscriptionStatus", "pair": "USDT/USD",
                      "status": "subscribed", "subscription": {"name": "ticker"}}),
        on_completed(220)
    ]


def test_wait_for_response_got_it():
    scheduler = TestScheduler()
    messages = scheduler.create_hot_observable(
        on_next(100, {'status': 'yolo'}),  # this will be missed since we only subscribe at 200
        on_next(250, {}),
        on_next(350, {'status': 'lala'}),
        on_next(450, {'status': 'yolo'}),
        on_next(550, {'status': 'yolo'}),
    )
    timeout = reactivex.interval(800, scheduler=scheduler)

    def is_match(m):
        return m.get('status') == 'yolo'

    results = scheduler.start(lambda: messages.pipe(wait_for_response(is_match, timeout)))

    assert results.messages == [
        on_next(450, {'status': 'yolo'}),
        on_completed(450),
    ]


def test_wait_for_response_timeout():
    scheduler = TestScheduler()
    messages = scheduler.create_hot_observable(
        on_next(100, {'status': 'yolo'}),  # this will be missed since we only subscribe at 200
        on_next(250, {}),
        on_next(350, {'status': 'lala'}),
        on_next(450, {'status': 'yolo'}),
        on_next(550, {'status': 'yolo'}),
    )
    timeout = reactivex.interval(230, scheduler=scheduler)

    def is_match(m):
        return m.get('status') == 'yolo'

    results = scheduler.start(lambda: messages.pipe(wait_for_response(is_match, timeout)))

    assert results.messages == [
        on_error(200 + 230, TimeoutError()),
    ]


def test_match_builder():
    matcher = build_matcher(42)
    assert not matcher([]), 'Lists should never match'
    assert not matcher({"reqid": 5})
    assert matcher({"reqid": 42})

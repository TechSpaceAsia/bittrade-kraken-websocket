import pytest
import reactivex
from reactivex.testing import ReactiveTest, TestScheduler
from reactivex.testing.subscription import Subscription

from bittrade_kraken_websocket.events.events import EVENT_SUBSCRIBE
from bittrade_kraken_websocket.events.request_response import request_response, _response_ok, RequestResponseError, \
    wait_for_response, build_match_checker
from tests.helpers.from_sample import from_sample

on_next = ReactiveTest.on_next
on_error = ReactiveTest.on_error
on_completed = ReactiveTest.on_completed
subscribe = ReactiveTest.subscribe


def test_request_response_timeout_using_next():
    scheduler = TestScheduler()
    sender = scheduler.create_observer()
    messages = scheduler.create_hot_observable(
        on_next(300, {"event": "addOrderStatus", "reqid": 9}),
        on_next(400, {"event": "incorrectStatus", "reqid": 100}),
    )
    timeout = scheduler.create_hot_observable(on_next(800, None))
    factory = request_response(sender, messages, timeout, "addOrder")

    def create():
        return factory({"abc": 42}, 100)

    result = scheduler.start(create)
    assert len(result.messages) == 1
    assert result.messages[0].time == 800
    assert type(result.messages[0].value.exception) == TimeoutError

    assert messages.subscriptions == [Subscription(200.0, 800.0)]
    assert timeout.subscriptions == [Subscription(200.0, 800.0)]


def test_request_response_timeout_using_error():
    scheduler = TestScheduler()
    sender = scheduler.create_observer()
    messages = scheduler.create_hot_observable(
        on_next(300, {"event": "addOrderStatus", "reqid": 9}),
        on_next(600, {"event": "editOrderStatus", "reqid": 100}),
    )
    timeout = scheduler.create_hot_observable(on_error(400, TimeoutError()))
    factory = request_response(sender, messages, timeout, "editOrder")

    def create():
        return factory({"abc": 42}, 100)

    result = scheduler.start(create)
    assert len(result.messages) == 1
    assert result.messages[0].time == 400
    assert type(result.messages[0].value.exception) == TimeoutError

    assert messages.subscriptions == [Subscription(200.0, 400.0)]
    assert timeout.subscriptions == [Subscription(200.0, 400.0)]


def test_request_response_success():
    scheduler = TestScheduler()
    sender = scheduler.create_observer()
    messages = scheduler.create_hot_observable(
        on_next(300, {"event": "addOrderStatus", "reqid": 9}),
        on_next(400, {"event": "incorrectStatus", "reqid": 100}),
        on_next(500, {"event": "addOrderStatus", "reqid": 100, "more": "stuff"}),
        on_next(600, {"event": "addOrderStatus", "reqid": 100, "more": "stuff again"}),
    )
    timeout = scheduler.create_hot_observable(on_next(800, None))
    factory = request_response(sender, messages, timeout, "addOrder")

    def create():
        return factory({"abc": 42}, 100)

    result = scheduler.start(create)
    assert result.messages == [
        on_next(500, {"event": "addOrderStatus", "reqid": 100, "more": "stuff"}),
        on_completed(500)
    ]

    assert messages.subscriptions == [Subscription(200.0, 500.0)]
    assert timeout.subscriptions == [Subscription(200.0, 500.0)]


def test_request_response_success():
    scheduler = TestScheduler()
    sender = scheduler.create_observer()
    messages = scheduler.create_hot_observable(
        on_next(300, {"event": "addOrderStatus", "reqid": 9}),
        on_next(400, {"event": "incorrectStatus", "reqid": 100}),
        on_next(500, {"event": "addOrderStatus", "reqid": 100, "more": "stuff"}),
        on_next(600, {"event": "addOrderStatus", "reqid": 100, "more": "stuff again"}),
    )
    timeout = scheduler.create_cold_observable(on_next(800, None))
    factory = request_response(sender, messages, timeout, "addOrder")

    def create():
        return factory({"abc": 42}, 100)

    result = scheduler.start(create)
    assert result.messages == [
        on_next(500, {"event": "addOrderStatus", "reqid": 100, "more": "stuff"}),
        on_completed(500)
    ]

    assert messages.subscriptions == [Subscription(200.0, 500.0)]
    assert timeout.subscriptions == [Subscription(200.0, 500.0)]


def test_request_response_success_repeat_same_id():
    scheduler = TestScheduler()
    sender = scheduler.create_observer()
    messages = scheduler.create_hot_observable(
        on_next(300, {"event": "addOrderStatus", "reqid": 9}),
        on_next(400, {"event": "incorrectStatus", "reqid": 100}),
        on_next(500, {"event": "addOrderStatus", "reqid": 100, "more": "stuff"}),
        on_next(600, {"event": "addOrderStatus", "reqid": 100, "more": "stuff again"}),
        on_next(700, {"event": "addOrderStatus", "reqid": 9}),
        on_next(800, {"event": "incorrectStatus", "reqid": 100}),
        on_next(900, {"event": "addOrderStatus", "reqid": 100, "more": "LALA"}),
        on_next(1000, {"event": "addOrderStatus", "reqid": 100, "more": "stuff again"}),
    )
    # Important! This confirms that the timeout is "per request" since we are still live at t=900. As long as the timeout isn't exceeded between t=0-500 and t=680-900
    timeout = scheduler.create_cold_observable(on_next(800, None))
    factory = request_response(sender, messages, timeout, "addOrder")

    o1 = factory({"abc": 42}, 100)
    o2 = factory({"abc": 43}, 100)
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

    assert messages.subscriptions == [Subscription(0.0, 500.0), Subscription(680.0, 900)]
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


def test_request_response_status_error():
    scheduler = TestScheduler()
    sender = scheduler.create_observer()
    messages = scheduler.create_hot_observable(
        on_next(300, {"event": "addOrderStatus", "reqid": 9, "status": "error"}),
        on_next(400, {"event": "incorrectStatus", "reqid": 100, "status": "error"}),
        on_next(500, {"event": "addOrderStatus", "reqid": 100, "status": "error", "more": "stuff"}),
        on_next(600, {"event": "addOrderStatus", "reqid": 100, "more": "stuff again"}),
    )
    timeout = scheduler.create_hot_observable(on_next(800, None))
    factory = request_response(sender, messages, timeout, "addOrder", raise_on_status=True)

    def create():
        return factory({"abc": 42}, 100)

    result = scheduler.start(create)
    assert result.messages[0].value.kind == 'E'
    assert result.messages[0].time == 500


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
    # Important! This confirms that the timeout is "per request" since we are still live at t=900. As long as the timeout isn't exceeded between t=0-500 and t=680-900
    timeout = scheduler.create_cold_observable(on_next(400, None))

    factory = request_response(sender, messages, timeout, "addOrder")

    o1 = factory({"abc": 42}, 100)
    o2 = factory({"abc": 43}, 5)
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
        sender, messages, timeout, event_type=EVENT_SUBSCRIBE
    )
    results = scheduler.start(
        lambda: caller({'event': 'subscribe', 'pair': ['USDT/USD'], 'subscription': {'name': 'ticker'}}))

    assert results.messages == [
        on_next(220, {"channelID": 1028, "channelName": "ticker", "event": "subscriptionStatus", "pair": "USDT/USD",
                      "status": "subscribed", "subscription": {"name": "ticker"}}),
        on_completed(220)
    ]


def test_request_response_subscription_wrong_pair():
    scheduler = TestScheduler()
    sender = scheduler.create_observer()
    messages = scheduler.create_hot_observable(
        from_sample('../samples/subscribe.jsonl')
    )
    timeout = scheduler.create_cold_observable(on_next(300, None))
    caller = request_response(
        sender, messages, timeout, event_type=EVENT_SUBSCRIBE
    )
    results = scheduler.start(
        lambda: caller({'event': 'subscribe', 'pair': ['XBT/USD'], 'subscription': {'name': 'ticker'}}))

    assert results.messages == [
        on_error(500, TimeoutError())
    ]


def test_request_response_subscription_multiple_pairs():
    scheduler = TestScheduler()
    sender = scheduler.create_observer()
    messages = scheduler.create_hot_observable(
        from_sample('../samples/subscribe.jsonl')
    )
    timeout = scheduler.create_cold_observable(on_next(900, None))
    caller = request_response(
        sender, messages, timeout, event_type=EVENT_SUBSCRIBE
    )
    results = scheduler.start(
        lambda: caller({'event': 'subscribe', 'pair': ['USDT/USD', 'XRP/USD'], 'subscription': {'name': 'ticker'}}))

    assert results.messages == [
        on_next(220, {"channelID": 1028, "channelName": "ticker", "event": "subscriptionStatus", "pair": "USDT/USD",
                      "status": "subscribed", "subscription": {"name": "ticker"}}),
        on_next(270, {"channelID": 900, "channelName": "ticker", "event": "subscriptionStatus", "pair": "XRP/USD",
                      "status": "subscribed", "subscription": {"name": "ticker"}}),
        on_completed(270)
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
    matcher = build_match_checker({'event': 'abc'})
    assert not matcher([]), 'Lists should never match'
    matcher = build_match_checker({
        'event': EVENT_SUBSCRIBE,
        'suscription'
    })
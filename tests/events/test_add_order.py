import dataclasses
from decimal import Decimal
from unittest.mock import MagicMock

import pytest
import reactivex
from reactivex import operators
from reactivex.testing import ReactiveTest, TestScheduler
from reactivex.testing.subscription import Subscription
from bittrade_kraken_websocket.channels.open_orders import OpenOrdersPayloadEntryDescr

from bittrade_kraken_websocket.events.add_order import (
    AddOrderRequest,
    create_order_lifecycle,
    order_related_messages_only,
)
from bittrade_kraken_websocket.events.models.order import (
    Order,
    OrderSide,
    OrderStatus,
    OrderType,
)
from bittrade_kraken_websocket.events.request_response import (
    RequestResponseError,
)
from tests.helpers.from_sample import from_sample

on_next = ReactiveTest.on_next
on_error = ReactiveTest.on_error
on_completed = ReactiveTest.on_completed
subscribe = ReactiveTest.subscribe


request_sample: AddOrderRequest = AddOrderRequest(
    **{
        "reqid": 5,
        "ordertype": OrderType.limit,
        "type": OrderSide.buy,
        "price": "10",
        "volume": "1",
        "pair": "USDT/USD",
        "oflags": "",
    }
)


def test_pydantic_parse_description():
    descr = {
        "close": None,
        "leverage": None,
        "order": "buy 30.00000000 USDT/USD @ limit 0.99980000",
        "ordertype": "limit",
        "pair": "USDT/USD",
        "price": "0.99980000",
        "price2": "0.00000000",
        "type": "buy",
    }
    parsed = OpenOrdersPayloadEntryDescr(**descr)

    assert parsed.type == OrderSide.buy
    assert parsed.price == Decimal("0.9998000")


def test_create_order_lifecycle():
    scheduler = TestScheduler()
    socket = MagicMock()
    messages = scheduler.create_hot_observable(
        from_sample("addOrder_messages_feed.json", start_time=0, time_interval=2)
    )

    def create():
        return create_order_lifecycle(
            (
                request_sample,
                socket,
            ),
            messages,
        )

    result = scheduler.start(create, created=0.1, subscribed=0.2)
    expected = [
        on_next(
            2,
            Order(
                order_id="OCI7RW-HMJJ2-WMMJBE",
                status=OrderStatus.submitted,
                description="buy 10.00000000 USDTUSD @ limit 0.9980",
                side=OrderSide.buy,
                order_type=OrderType.limit,
                price=Decimal(
                    "10"
                ),  # this 10 comes from the sample request, at this stage we do not yet have a value from Kraken, so using request's details
            ),
        ),
        on_next(
            4,
            Order(
                order_id="OCI7RW-HMJJ2-WMMJBE",
                status=OrderStatus.pending,
                price=Decimal("0.99800000"),
                price2=Decimal("0.00000000"),
                description="buy 10.00000000 USDTUSD @ limit 0.9980",
                order_type=OrderType.limit,
                volume="10.00000000",
                side=OrderSide.buy,
            ),
        ),
        on_next(
            6,
            Order(
                order_id="OCI7RW-HMJJ2-WMMJBE",
                status=OrderStatus.open,
                price=Decimal("0.99800000"),
                description="buy 10.00000000 USDTUSD @ limit 0.9980",
                order_type=OrderType.limit,
                price2=Decimal("0.00000000"),
                volume="10.00000000",
                side=OrderSide.buy,
            ),
        ),
        on_completed(6),
    ]
    assert result.messages == expected
    # for i, assertion in enumerate(zip(result.messages, expected)):
    #     message, exp = assertion
    #     # assert message == exp, f"Error on message {i+1}"
    #     assert message.value.value == exp.value.value
    #     assert message.time == exp.time
    #     assert message.value.kind == exp.value.kind
    #     assert message.value == exp.value

    socket.send_json.assert_called_once_with(dataclasses.asdict(request_sample))


def test_create_order_lifecycle_timeout():
    scheduler = TestScheduler()
    socket = MagicMock()
    messages = scheduler.create_hot_observable(
        from_sample("addOrder_messages_feed.json", start_time=10, time_interval=6)
    )

    def create():
        return create_order_lifecycle(
            (
                request_sample,
                socket,
            ),
            messages,
        )

    result = scheduler.start(create, created=1, subscribed=10)

    assert result.messages == [on_error(15, Exception("Timeout"))]


def test_order_related_only():
    scheduler = TestScheduler()
    messages = scheduler.create_hot_observable(
        on_next(400, {}),
        on_next(500, []),
        on_next(600, [[], "lala"]),
        on_next(700, [[], "openOrders"]),
        on_next(800, [[{}], "openOrders"]),
        on_next(900, [[{"abcd": 42}], "openOrders"]),  # wrong id
        on_next(910, [[{"real-order-id": 42}], "openOrders"]),
        on_next(920, [[{"real-order-id": 43, "order": 42}], "openOrders"]),
        on_next(930, [[{"real-order-id": 43}], "wrong"]),  # wrong event
    )
    results = scheduler.start(
        lambda: messages.pipe(order_related_messages_only("real-order-id"))
    )

    assert results.messages == [
        on_next(910, 42),
        on_next(920, 43),
    ]


"""ALL THE BELOW TESTS WERE TAKEN FROM PREVIOUS ATTEMPTS; NEED TO EDIT THEM"""


def _test_add_order_success():
    scheduler = TestScheduler()
    sender = scheduler.create_observer()
    options = AddOrderRequest(
        price="0.9980", volume="10", ordertype="limit", type="buy", pair="USDT/USD"
    )
    socket_messages = scheduler.create_hot_observable(
        [
            on_next(
                250,
                {
                    "descr": "buy 10.00000000 USDTUSD @ limit 0.9980",
                    "event": "not the right event",
                    "reqid": 5,
                    "status": "ok",
                    "txid": "OXW22X-FYBXP-JQDBJT",
                },
            ),
            on_next(
                280,
                {
                    "descr": "buy 10.00000000 USDTUSD @ limit 0.9980",
                    "event": "addOrderStatus",
                    "reqid": 500,  # wrong id
                    "status": "ok",
                    "txid": "OXW22X-FYBXP-JQDBJT",
                },
            ),
            on_next(
                300,
                {
                    "descr": "this should work",
                    "event": "addOrderStatus",
                    "reqid": 5,
                    "status": "ok",
                    "txid": "OXW22X-FYBXP-JQDBJT",
                },
            ),
            on_next(350, ["some rubbish"]),
            on_next(
                360,
                {
                    "descr": "different reqid after first",
                    "event": "addOrderStatus",
                    "reqid": 9001,  # another reqid
                    "status": "ok",
                    "txid": "LALA",
                },
            ),
            on_next(
                370,
                [
                    [
                        {
                            "WRONG LEVER": {
                                "avg_price": "0.00000000",
                                "cost": "0.00000000",
                                "descr": {
                                    "close": None,
                                    "leverage": None,
                                    "order": "buy 10.00000000 USDT/USD @ limit 0.99800000",
                                    "ordertype": "limit",
                                    "pair": "USDT/USD",
                                    "price": "0.99800000",
                                    "price2": "0.00000000",
                                    "type": "buy",
                                },
                                "expiretm": None,
                                "fee": "0.00000000",
                                "limitprice": "0.00000000",
                                "misc": "",
                                "oflags": "fciq",
                                "opentm": "1671098777.370552",
                                "refid": None,
                                "starttm": None,
                                "status": "pending",
                                "stopprice": "0.00000000",
                                "timeinforce": "GTC",
                                "userref": 0,
                                "vol": "10.00000000",
                                "vol_exec": "0.00000000",
                            }
                        }
                    ],
                    "openOrders",
                    {"sequence": 4},
                ],
            ),
            on_next(
                400,
                [
                    [
                        {
                            "OXW22X-FYBXP-JQDBJT": {
                                "avg_price": "0.00000000",
                                "cost": "0.00000000",
                                "descr": {
                                    "close": None,
                                    "leverage": None,
                                    "order": "buy 10.00000000 USDT/USD @ limit 0.99800000",
                                    "ordertype": "limit",
                                    "pair": "USDT/USD",
                                    "price": "0.99800000",
                                    "price2": "0.00000000",
                                    "type": "buy",
                                },
                                "expiretm": None,
                                "fee": "0.00000000",
                                "limitprice": "0.00000000",
                                "misc": "",
                                "oflags": "fciq",
                                "opentm": "1671098777.370552",
                                "refid": None,
                                "starttm": None,
                                "status": "pending",
                                "stopprice": "0.00000000",
                                "timeinforce": "GTC",
                                "userref": 0,
                                "vol": "10.00000000",
                                "vol_exec": "0.00000000",
                            }
                        }
                    ],
                    "openOrders",
                    {"sequence": 4},
                ],
            ),
            on_next(
                500,
                [
                    [{"OXW22X-FYBXP-JQDBJT": {"status": "open", "userref": 0}}],
                    "openOrders",
                    {"sequence": 5},
                ],
            ),
            on_next(
                600,
                [
                    [{"OXW22X-FYBXP-JQDBJT": {"status": "canceled", "userref": 0}}],
                    "openOrders",
                    {"sequence": 6},
                ],
            ),
        ]
    )
    caller = add_order_factory_full(
        sender, socket_messages, scheduler.create_cold_observable(on_next(1000, None))
    )
    result = scheduler.start(lambda: caller(options, 5))
    expected = [
        on_next(300, Order(order_id="OXW22X-FYBXP-JQDBJT", status="submitted")),
        on_next(400, Order(order_id="OXW22X-FYBXP-JQDBJT", status="pending")),
        on_next(500, Order(order_id="OXW22X-FYBXP-JQDBJT", status="open")),
        on_completed(500),
    ]
    assert result.messages == expected
    assert socket_messages.subscriptions == [
        from_to(200, 500),
        from_to(200, 300),
    ]
    # Confirm that the sender was triggered as soon as we subscribed
    assert sender.messages[0].time == 200
    m = sender.messages[0].value
    assert m.kind == "N"
    assert m.value["reqid"] == 5
    assert m.value["volume"] == "10"


def _test_add_order_success_dispose():
    """Dispose before even first order, should stop all subs"""
    scheduler = TestScheduler()
    sender = scheduler.create_observer()
    options = AddOrderRequest(
        price="0.9980", volume="10", ordertype="limit", type="buy", pair="USDT/USD"
    )
    socket_messages = scheduler.create_hot_observable(
        [
            on_next(
                250,
                {
                    "descr": "buy 10.00000000 USDTUSD @ limit 0.9980",
                    "event": "not the right event",
                    "reqid": 5,
                    "status": "ok",
                    "txid": "OXW22X-FYBXP-JQDBJT",
                },
            ),
            on_next(
                300,
                {
                    "descr": "this should work",
                    "event": "addOrderStatus",
                    "reqid": 5,
                    "status": "ok",
                    "txid": "OXW22X-FYBXP-JQDBJT",
                },
            ),
            on_next(
                400,
                [
                    [
                        {
                            "OXW22X-FYBXP-JQDBJT": {
                                "avg_price": "0.00000000",
                                "cost": "0.00000000",
                                "descr": {
                                    "close": None,
                                    "leverage": None,
                                    "order": "buy 10.00000000 USDT/USD @ limit 0.99800000",
                                    "ordertype": "limit",
                                    "pair": "USDT/USD",
                                    "price": "0.99800000",
                                    "price2": "0.00000000",
                                    "type": "buy",
                                },
                                "expiretm": None,
                                "fee": "0.00000000",
                                "limitprice": "0.00000000",
                                "misc": "",
                                "oflags": "fciq",
                                "opentm": "1671098777.370552",
                                "refid": None,
                                "starttm": None,
                                "status": "pending",
                                "stopprice": "0.00000000",
                                "timeinforce": "GTC",
                                "userref": 0,
                                "vol": "10.00000000",
                                "vol_exec": "0.00000000",
                            }
                        }
                    ],
                    "openOrders",
                    {"sequence": 4},
                ],
            ),
            on_next(
                500,
                [
                    [{"OXW22X-FYBXP-JQDBJT": {"status": "open", "userref": 0}}],
                    "openOrders",
                    {"sequence": 5},
                ],
            ),
            on_next(
                600,
                [
                    [{"OXW22X-FYBXP-JQDBJT": {"status": "canceled", "userref": 0}}],
                    "openOrders",
                    {"sequence": 6},
                ],
            ),
        ]
    )
    caller = add_order_factory_full(
        sender, socket_messages, scheduler.create_cold_observable(on_next(1000, None))
    )
    result = scheduler.start(lambda: caller(options, 5), disposed=280)
    expected = []
    assert result.messages == expected
    assert socket_messages.subscriptions == [
        from_to(200, 280),
        from_to(200, 280),
    ]


def _test_add_order_success_dispose():
    """Dispose before even first order, should stop all subs"""
    scheduler = TestScheduler()
    sender = scheduler.create_observer()
    options = AddOrderRequest(
        price="0.9980", volume="10", ordertype="limit", type="buy", pair="USDT/USD"
    )
    socket_messages = scheduler.create_hot_observable(
        [
            on_next(
                250,
                {
                    "descr": "buy 10.00000000 USDTUSD @ limit 0.9980",
                    "event": "not the right event",
                    "reqid": 5,
                    "status": "ok",
                    "txid": "OXW22X-FYBXP-JQDBJT",
                },
            ),
            on_next(
                300,
                {
                    "descr": "this should work",
                    "event": "addOrderStatus",
                    "reqid": 5,
                    "status": "ok",
                    "txid": "OXW22X-FYBXP-JQDBJT",
                },
            ),
            on_next(
                400,
                [
                    [
                        {
                            "OXW22X-FYBXP-JQDBJT": {
                                "avg_price": "0.00000000",
                                "cost": "0.00000000",
                                "descr": {
                                    "close": None,
                                    "leverage": None,
                                    "order": "buy 10.00000000 USDT/USD @ limit 0.99800000",
                                    "ordertype": "limit",
                                    "pair": "USDT/USD",
                                    "price": "0.99800000",
                                    "price2": "0.00000000",
                                    "type": "buy",
                                },
                                "expiretm": None,
                                "fee": "0.00000000",
                                "limitprice": "0.00000000",
                                "misc": "",
                                "oflags": "fciq",
                                "opentm": "1671098777.370552",
                                "refid": None,
                                "starttm": None,
                                "status": "pending",
                                "stopprice": "0.00000000",
                                "timeinforce": "GTC",
                                "userref": 0,
                                "vol": "10.00000000",
                                "vol_exec": "0.00000000",
                            }
                        }
                    ],
                    "openOrders",
                    {"sequence": 4},
                ],
            ),
            on_next(
                500,
                [
                    [{"OXW22X-FYBXP-JQDBJT": {"status": "open", "userref": 0}}],
                    "openOrders",
                    {"sequence": 5},
                ],
            ),
            on_next(
                600,
                [
                    [{"OXW22X-FYBXP-JQDBJT": {"status": "canceled", "userref": 0}}],
                    "openOrders",
                    {"sequence": 6},
                ],
            ),
        ]
    )
    caller = add_order_factory_full(
        sender, socket_messages, scheduler.create_cold_observable(on_next(1000, None))
    )
    result = scheduler.start(lambda: caller(options, 5), disposed=280)
    expected = []
    assert result.messages == expected
    assert socket_messages.subscriptions == [
        from_to(200, 280),
        from_to(200, 280),
    ]


def _test_add_order_failed():
    """If response to event is a failure, should error"""
    scheduler = TestScheduler()
    sender = scheduler.create_observer()
    options = AddOrderRequest(
        price="0.9980", volume="10", ordertype="limit", type="buy", pair="USDT/USD"
    )
    socket_messages = scheduler.create_hot_observable(
        on_next(
            250,
            {
                "errorMessage": "Unsupported field: 'refid' for the given msg type: add order",
                "event": "addOrderStatus",
                "pair": "USDT/USD",
                "reqid": 20,
                "status": "error",
            },
        )
    )
    caller = add_order_factory_full(
        sender, socket_messages, scheduler.create_cold_observable(on_next(1000, None))
    )
    result = scheduler.start(lambda: caller(options, 20))
    assert result.messages == [
        on_error(
            250,
            RequestResponseError(
                "Unsupported field: 'refid' for the given msg type: add order"
            ),
        )
    ]
    assert socket_messages.subscriptions == [
        from_to(200, 250),
        from_to(200, 250),
    ]


def _test_add_order_timeout_initial():
    """If response to event is a failure, should error"""
    scheduler = TestScheduler()
    sender = scheduler.create_observer()
    options = AddOrderRequest(
        price="0.9980", volume="10", ordertype="limit", type="buy", pair="USDT/USD"
    )
    socket_messages = scheduler.create_hot_observable(
        on_next(
            600,
            {
                "descr": "this should work if only it wasnt too late",
                "event": "addOrderStatus",
                "reqid": 50,
                "status": "ok",
                "txid": "OXW22X-FYBXP-JQDBJT",
            },
        )
    )
    caller = add_order_factory_full(
        sender, socket_messages, scheduler.create_cold_observable(on_next(300, None))
    )  # 200+300 < 600
    result = scheduler.start(lambda: caller(options, 50))
    m = result.messages[0]
    assert m.time == 500
    assert m.value.kind == "E"
    assert type(m.value.exception) == TimeoutError


def _test_add_order_timeout_details():
    """If response to event is a failure, should error"""
    scheduler = TestScheduler()
    sender = scheduler.create_observer()
    options = AddOrderRequest(
        price="0.9980", volume="10", ordertype="limit", type="buy", pair="USDT/USD"
    )
    socket_messages = scheduler.create_hot_observable(
        on_next(
            300,
            {
                "descr": "this should work if only it wasnt too late",
                "event": "addOrderStatus",
                "reqid": 50,
                "status": "ok",
                "txid": "OXW22X-FYBXP-JQDBJT",
            },
        ),
        on_next(
            600,
            [
                [{"OXW22X-FYBXP-JQDBJT": {"status": "open", "userref": 0}}],
                "openOrders",
                {"sequence": 5},
            ],
        ),
    )
    caller = add_order_factory_full(
        sender, socket_messages, scheduler.create_cold_observable(on_next(350, None))
    )  # 200+350 < 600
    result = scheduler.start(lambda: caller(options, 50))
    assert result.messages[0] == on_next(
        300, Order(order_id="OXW22X-FYBXP-JQDBJT", status="submitted")
    )
    m = result.messages[1]
    assert m.time == 550
    assert m.value.kind == "E"
    assert type(m.value.exception) == TimeoutError

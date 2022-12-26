from decimal import Decimal
from unittest.mock import MagicMock

import pytest
import reactivex
from reactivex import operators
from reactivex.testing import ReactiveTest, TestScheduler
from reactivex.testing.subscription import Subscription

from bittrade_kraken_websocket.events.add_order import AddOrderRequest, create_order_lifecycle
from bittrade_kraken_websocket.events.models.order import Order, OrderSide, OrderStatus, OrderType
from bittrade_kraken_websocket.events.request_response import request_response, _response_ok, RequestResponseError, \
    wait_for_response, build_matcher
from bittrade_kraken_websocket.events import request_response_factory
from tests.helpers.from_sample import from_sample

on_next = ReactiveTest.on_next
on_error = ReactiveTest.on_error
on_completed = ReactiveTest.on_completed
subscribe = ReactiveTest.subscribe


request_sample: AddOrderRequest = {
    "reqid": 5, 
    "ordertype": OrderType.limit,
    "type": OrderSide.buy,
    "price": "10",
    "volume": "1",
    "pair": "USDT/USD",
    "oflags": ""
}

def test_create_order_lifecycle():
    scheduler = TestScheduler()
    socket = MagicMock()
    messages = scheduler.create_hot_observable(
        from_sample('addOrder_messages_feed.json', start_time=0, time_interval=2)
    )

    request: AddOrderRequest = dict(request_sample) # type: ignore
    def create():
        return create_order_lifecycle((request, socket,), messages)

    result = scheduler.start(create, created=0.1, subscribed=0.2)

    assert result.messages == [
        on_next(2, Order(
            order_id='OCI7RW-HMJJ2-WMMJBE',
            status=OrderStatus.submitted,
            description = 'buy 10.00000000 USDTUSD @ limit 0.9980')),
        on_next(4, Order(
            order_id='OCI7RW-HMJJ2-WMMJBE',
            status=OrderStatus.pending,
            description='buy 10.00000000 USDTUSD @ limit 0.9980', volume=Decimal('10.00000000'))),
        on_next(6, Order(
            order_id='OCI7RW-HMJJ2-WMMJBE',
            status=OrderStatus.open,
            description='buy 10.00000000 USDTUSD @ limit 0.9980', volume=Decimal('10.00000000'))),
        on_completed(6)
    ]

    socket.send_json.assert_called_once_with(request)



def test_create_order_lifecycle_timeout():
    scheduler = TestScheduler()
    socket = MagicMock()
    messages = scheduler.create_hot_observable(
        from_sample('addOrder_messages_feed.json', start_time=10, time_interval=6)
    )

    request: AddOrderRequest = dict(request_sample) # type: ignore

    def create():
        return create_order_lifecycle((request, socket,), messages)

    result = scheduler.start(create, created=1, subscribed=10)

    assert result.messages == [
        on_error(15, Exception('Timeout'))
    ]

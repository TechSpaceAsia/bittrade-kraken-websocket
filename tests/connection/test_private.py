
import reactivex
from reactivex import operators, Observable
from reactivex.notification import OnError
from reactivex.testing import ReactiveTest, TestScheduler
from reactivex.testing.subscription import Subscription

from bittrade_kraken_websocket.connection.generic import EnhancedWebsocket, WEBSOCKET_STATUS
from bittrade_kraken_websocket.connection.private import add_token
from bittrade_kraken_websocket.connection.reconnect import retry_with_backoff
from bittrade_kraken_websocket.connection.status import WEBSOCKET_AUTHENTICATED
from bittrade_kraken_websocket.events.events import EVENT_SUBSCRIBE
from bittrade_kraken_websocket.events.request_response import request_response, _response_ok, RequestResponseError
from tests.helpers.from_sample import from_sample
from tests.helpers.subscriptions import from_to

on_next = ReactiveTest.on_next
on_error = ReactiveTest.on_error
on_completed = ReactiveTest.on_completed
subscribe = ReactiveTest.subscribe

def test_add_token():
    scheduler = TestScheduler()
    tokens = [1,4,10]
    def fac():
        scheduler.sleep(20)
        return tokens.pop(0)

    token_generator = reactivex.from_callable(fac)
    enhanced = [EnhancedWebsocket(None), EnhancedWebsocket(None), EnhancedWebsocket(None), EnhancedWebsocket(None)]
    socket_emitter = scheduler.create_hot_observable(
        on_next(300, [enhanced[0]]),
        on_next(350, [enhanced[1]]),
        on_next(460, [enhanced[2]]),
        on_next(880, [enhanced[3]]),
    )

    def create():
        return socket_emitter.pipe(
            add_token(token_generator)
        )

    results = scheduler.start(create)

    assert results.messages[:3] == [
        on_next(320, [enhanced[0], WEBSOCKET_STATUS, WEBSOCKET_AUTHENTICATED]),
        on_next(370, [enhanced[1], WEBSOCKET_STATUS, WEBSOCKET_AUTHENTICATED]),
        on_next(480, [enhanced[2], WEBSOCKET_STATUS, WEBSOCKET_AUTHENTICATED]),
    ]
    assert enhanced[0].token == 1
    assert enhanced[1].token == 4
    assert enhanced[2].token == 10
    m = results.messages[3]
    assert m.time == 880+20
    assert m.value.kind == 'E'




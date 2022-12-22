from reactivex.testing import ReactiveTest, TestScheduler

from bittrade_kraken_websocket.connection.enhanced_websocket import EnhancedWebsocket
from bittrade_kraken_websocket.connection.generic import WEBSOCKET_MESSAGE, WEBSOCKET_STATUS, WEBSOCKET_HEARTBEAT
from bittrade_kraken_websocket.connection.status import WEBSOCKET_OPENED, WEBSOCKET_CLOSED
from bittrade_kraken_websocket.messages.listen import keep_messages_only, keep_status_only, filter_new_socket_only

on_next = ReactiveTest.on_next
on_error = ReactiveTest.on_error
subscribe = ReactiveTest.subscribe


def test_listen_message_only():
    scheduler = TestScheduler()
    source = scheduler.create_hot_observable(
        on_next(300, ['a', WEBSOCKET_MESSAGE, 'b']),
        on_next(400, ['a', WEBSOCKET_STATUS, 'c']),
        on_next(500, ['b', WEBSOCKET_MESSAGE, '{"a": 42}']),
    )

    results = scheduler.start(lambda: source.pipe(
        keep_messages_only()
    ))

    assert results.messages == [
        on_next(300, 'b'),
        on_next(500, '{"a": 42}'),
    ]

def test_listen_status_only():
    scheduler = TestScheduler()
    source = scheduler.create_hot_observable(
        on_next(300, ['a', WEBSOCKET_MESSAGE, [1,2,3]]),
        on_next(410, ['b', WEBSOCKET_STATUS, WEBSOCKET_OPENED]),
        on_next(450, ['b', WEBSOCKET_STATUS, WEBSOCKET_CLOSED]),
        on_next(500, ['b', WEBSOCKET_MESSAGE, {"a": 42}]),
    )

    results = scheduler.start(lambda: source.pipe(
        keep_status_only()
    ))
    assert results.messages == [
        on_next(410, WEBSOCKET_OPENED),
        on_next(450, WEBSOCKET_CLOSED),
    ]


def test_listen_new_socket_only():
    scheduler = TestScheduler()
    first, second = EnhancedWebsocket(None), EnhancedWebsocket(None)  # noqa
    source = scheduler.create_hot_observable(
        on_next(300, [first, WEBSOCKET_MESSAGE, [1,2,3]]),
        on_next(410, [first, WEBSOCKET_STATUS, WEBSOCKET_OPENED]),
        on_next(450, [second, WEBSOCKET_STATUS, WEBSOCKET_CLOSED]),
        on_next(500, [second, WEBSOCKET_MESSAGE, {"a": 42}]),
    )
    assert scheduler.start(lambda: source.pipe(
        filter_new_socket_only()
    )).messages == [
        on_next(300, first),
        on_next(450, second)
    ]


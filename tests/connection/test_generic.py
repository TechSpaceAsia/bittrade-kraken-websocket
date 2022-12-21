from unittest.mock import MagicMock, patch

from reactivex.scheduler import CurrentThreadScheduler

from bittrade_kraken_websocket.connection.generic import websocket_connection, WebSocketApp


def test_on_open():
    connection = websocket_connection()

    def check(x):
        print(x)
    @patch.object(WebSocketApp, '__new__')
    def test(new_function):
        new_function.side_effect = print('YOOOO')
        obs = websocket_connection().subscribe(
            on_next=check, scheduler=CurrentThreadScheduler()
        )
        print(obs)
    test()

import reactivex
from reactivex.operators import take, publish

from bittrade_kraken_websocket.connection.reconnect import repeat_with_backoff
from bittrade_kraken_websocket.connection.public import public_websocket_connection
import time


stable = reactivex.interval(5.0).pipe(take(1))
socket = public_websocket_connection(
    repeat_with_backoff(),
    publish()
)

socket.connect()

reactivex.interval(2*60).pipe(take(1))
import logging

import reactivex
from reactivex.operators import take, publish
from reactivex.scheduler import ThreadPoolScheduler
from rich.logging import RichHandler

from bittrade_kraken_websocket.connection.reconnect import retry_with_backoff
from bittrade_kraken_websocket.connection.public import public_websocket_connection
import time

from bittrade_kraken_websocket.development import info_observer, debug_observer
from bittrade_kraken_websocket.events.subscribe import subscribe_ticker

console = RichHandler()
console.setLevel(logging.DEBUG)
logger = logging.getLogger(
    'bittrade_kraken_websocket'
)
logger.setLevel(logging.DEBUG)
logger.addHandler(console)

stable = reactivex.interval(100.0).pipe(take(1))
socket = public_websocket_connection().pipe(
    retry_with_backoff(),
    publish()
)
pool_scheduler = ThreadPoolScheduler()
socket.subscribe(debug_observer('SOCKET'))

sub = socket.connect(scheduler=pool_scheduler)

# Since we're not doing anything, websocket will eventually error
while True:
    pass
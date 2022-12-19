import logging

import reactivex
from reactivex import operators
from reactivex.operators import take, publish, do_action, do, concat, skip, flat_map
from reactivex.scheduler import ThreadPoolScheduler
from reactivex.subject import BehaviorSubject
from rich.logging import RichHandler

from bittrade_kraken_websocket.connection.connection_operators import connected_socket
from bittrade_kraken_websocket.connection.reconnect import retry_with_backoff
from bittrade_kraken_websocket.connection.public import public_websocket_connection
from bittrade_kraken_websocket.connection.generic import websocket_connection

from bittrade_kraken_websocket.development import info_observer, debug_observer

console = RichHandler()
console.setLevel(logging.DEBUG)
logger = logging.getLogger(
    'bittrade_kraken_websocket'
)
logger.setLevel(logging.DEBUG)
logger.addHandler(console)

stable = reactivex.interval(100.0) # this "stable" trigger lasts longer than the time until error so the connection will never be considered stable. You will therefore see the backoff in action
connection = public_websocket_connection().pipe(
    do(info_observer('ERRORS WILL SHOW HERE')),
    retry_with_backoff(stabilized=stable),
    publish()
)
pool_scheduler = ThreadPoolScheduler()
connection.subscribe(info_observer('SOCKET'))

sub = connection.connect(scheduler=pool_scheduler)

# Since we're not doing anything, websocket will eventually error after 60sec
while True:
    pass
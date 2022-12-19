import functools
import logging

import reactivex
from reactivex import operators
from reactivex.operators import take, publish, do_action, do, concat, skip, flat_map
from reactivex.scheduler import ThreadPoolScheduler, TimeoutScheduler
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

i = 0
def fac(scheduler):
    global i
    i += 1
    if i <= 6:
        stable_duration = 5.0 # The first 6 times the error will occur faster than stabilization; you will see backoff
    else:
        stable_duration = 2.0 # Stable will trigger so no more backoff
    return reactivex.interval(stable_duration)
stable = reactivex.defer(fac)

# Binance errors when we send gibberish unlike Kraken
connection = websocket_connection('wss://testnet.binance.vision/ws').pipe(
    do(debug_observer('ERRORS WILL SHOW HERE')),
    retry_with_backoff(stable),
    publish()
)
timeout_scheduler = TimeoutScheduler()
def send_gibberish(m):
    logger.warning('Will send gibberish in 3 seconds %s', m)
    timeout_scheduler.schedule_relative(3.0, lambda *args: m.socket.send('gibberish'))

connection.pipe(
    connected_socket(),
).subscribe(on_next=send_gibberish)

pool_scheduler = ThreadPoolScheduler()
sub = connection.connect(scheduler=pool_scheduler)

timeout_scheduler.schedule_relative(120, lambda: sub.dispose())

# Since we're not doing anything, websocket will eventually error
while True:
    pass
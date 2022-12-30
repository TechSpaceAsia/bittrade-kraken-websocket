import logging
import time

import reactivex
from reactivex.operators import publish, do_action
from reactivex.scheduler import ThreadPoolScheduler
from rich.logging import RichHandler

from bittrade_kraken_websocket.connection.reconnect import retry_with_backoff
from bittrade_kraken_websocket.connection.public import public_websocket_connection

from bittrade_kraken_websocket.development import info_observer

console = RichHandler()
console.setLevel(logging.DEBUG)
logger = logging.getLogger(
    'bittrade_kraken_websocket'
)
logger.setLevel(logging.DEBUG)
logger.addHandler(console)

stable = reactivex.interval(100.0) # this "stable" trigger lasts longer than the time until error so the connection will never be considered stable. You will therefore see the backoff in action

with_backoff = public_websocket_connection(reconnect=False, shared=False).pipe(
    do_action(on_error=lambda x: logger.info('ERRORS WILL SHOW HERE %s', x)),
    retry_with_backoff(stabilized=stable),
    publish()
)
pool_scheduler = ThreadPoolScheduler()
with_backoff.subscribe(info_observer('SOCKET'))

sub = with_backoff.connect(scheduler=pool_scheduler)
assert sub is not None
time.sleep(300)

sub.dispose()
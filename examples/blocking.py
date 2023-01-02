import logging
import time
from typing import cast

from reactivex.operators import share, take
from reactivex.abc import DisposableBase
from reactivex import scheduler
from rich.logging import RichHandler

from bittrade_kraken_websocket import (public_websocket_connection,
                                       subscribe_ticker)
from bittrade_kraken_websocket.development import (LogOnDisposeDisposable,
                                                   debug_observer,
                                                   info_observer)
from bittrade_kraken_websocket.operators import (filter_new_socket_only,
                                                 keep_messages_only)

console = RichHandler()
console.setLevel(logging.DEBUG)
logger = logging.getLogger(
    'bittrade_kraken_websocket'
)
logger.setLevel(logging.DEBUG)
logger.addHandler(console)
formatter = logging.Formatter('[%(levelname)s] (%(threadName)-9s) %(message)s')
console.setFormatter(formatter)
socket_connection = public_websocket_connection()
# socket_connection = public_websocket_connection(scheduler=scheduler.CurrentThreadScheduler())  # <- uncomment this to get a blocking websocket connection
messages = socket_connection.pipe(
    keep_messages_only(),
    share()  # Usually best to share messages to avoid overhead
)
socket_connection.pipe(
    filter_new_socket_only(),
    subscribe_ticker('USDT/USD', messages)
).subscribe(debug_observer('TICKER'))
socket_connection.connect()
print('You ll see me immediately if not overriding scheduler')
import logging
import time
from typing import cast

from reactivex.operators import share
from reactivex.abc import DisposableBase
from rich.logging import RichHandler

from bittrade_kraken_websocket import (public_websocket_connection,
                                       subscribe_spread, subscribe_ticker, subscribe_trade)
from bittrade_kraken_websocket.development import (LogOnDisposeDisposable,
                                                   debug_observer,
                                                   info_observer)
from bittrade_kraken_websocket.operators import (filter_new_socket_only,
                                                 keep_messages_only)

console = RichHandler()
console.setLevel(logging.INFO)  # <- if you wish to see subscribe/unsubscribe and raw messages, change to DEBUG
logger = logging.getLogger(
    'bittrade_kraken_websocket'
)
logger.setLevel(logging.DEBUG)
logger.addHandler(console)
socket_connection = public_websocket_connection()
messages = socket_connection.pipe(
    keep_messages_only(),
    share()  # Usually best to share messages to avoid overhead
)
messages.subscribe(debug_observer('ALL MESSAGES'))
# Subscribe to multiple channels only when socket connects
ready = socket_connection.pipe(
    filter_new_socket_only(),
    share()
)
# subscribe_to_channel gives an observable with only the messages from that channel
# ready.pipe(
#     subscribe_ticker('USDT/USD', messages)
# ).subscribe(
#     info_observer('TICKER USDT')
# )

# Disposing of the subscription to channel triggers sending the "unsubscribe" socket message.
# The LogOnDisposeDisposable is just a helper to print a message on disposal - you will likely not need it
to_be_disposed = LogOnDisposeDisposable(
    [ready.pipe(
        # subscribe_spread('XRP/USD', messages),
        subscribe_trade('USDC/USD', messages),
    ).subscribe(
        # info_observer('SPREAD XRP')
        info_observer('TRADE USDC'),
    )],
    message='From here on, TRADE messages should not appear anymore',
    # message='From here on, SPREAD XRP messages should not appear anymore',
)

# Ready, start connecting and subscribing to channels
sub = socket_connection.connect()

time.sleep(30)
to_be_disposed.dispose() # the "messages" will now show only "ticker", no more "spread"
time.sleep(20)
assert sub is not None
sub.dispose() # because all the subscriptions here are children of the socket connectable observable, everything will get cleaned up and websocket closed

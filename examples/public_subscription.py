import sys
import time
import logging
import orjson
import signal

import reactivex
from reactivex import operators, Observable
from reactivex.operators import publish, share, take, do_action, ignore_elements
from reactivex.scheduler import TimeoutScheduler

from bittrade_kraken_websocket.channels import CHANNEL_TICKER, CHANNEL_OHLC, CHANNEL_SPREAD
from bittrade_kraken_websocket.connection.generic import EnhancedWebsocket
from bittrade_kraken_websocket.connection.public import public_websocket_connection
from rich import print as pretty_print
from rich.logging import RichHandler
from concurrent.futures import ThreadPoolExecutor

from bittrade_kraken_websocket.connection.connection_operators import connected_socket
from bittrade_kraken_websocket.development import debug_observer, info_observer
from bittrade_kraken_websocket.events.subscribe import subscribe_to_channel
from bittrade_kraken_websocket.messages.listen import keep_messages_only

console = RichHandler()
console.setLevel(logging.DEBUG)
logger = logging.getLogger(
    'bittrade_kraken_websocket'
)
logger.setLevel(logging.DEBUG)
logger.addHandler(console)
socket_connection = public_websocket_connection(
    json_messages=True
).pipe(
    publish()
)
messages = socket_connection.pipe(
    keep_messages_only(),
    share()
)
messages.subscribe(debug_observer('ALL MESSAGES'))
# Subscribe to multiple channels only when socket connects
connected = socket_connection.pipe(
    connected_socket(),
)
# subscribe_to_channel gives an observable with only the messages from that channel
connected.pipe(
    subscribe_to_channel(messages, channel=CHANNEL_TICKER, pair='USDT/USD')
).subscribe(
    info_observer('TICKER USDT')
)

# Disposing of the subscription to channel triggers sending the "unsubscribe" socket message. If needed you can get around that using share() or publish()
to_be_disposed = connected.pipe(
    subscribe_to_channel(messages, channel=CHANNEL_SPREAD, pair='XRP/USD')
).subscribe(
    info_observer('SPREAD XRP')
)

sub = socket_connection.connect()

scheduler = TimeoutScheduler()

scheduler.schedule_relative(20, lambda *_: to_be_disposed.dispose())  # the "messages" will now show only "ticker", no more "spread"
scheduler.schedule_relative(60, lambda *_: sub.dispose()) # because all the subscriptions here are children of the socket connectable observable, everything will get cleaned up and websocket closed

import logging

from reactivex.operators import share
from reactivex.scheduler import TimeoutScheduler
from rich.logging import RichHandler

from bittrade_kraken_websocket.channels import (CHANNEL_SPREAD,
                                                CHANNEL_TICKER)
from bittrade_kraken_websocket.operators import (
    filter_new_socket_only)
from bittrade_kraken_websocket.connection.public import \
    public_websocket_connection
from bittrade_kraken_websocket.development import debug_observer, info_observer, LogOnDisposeDisposable
from bittrade_kraken_websocket.channels.subscribe import subscribe_to_channel
from bittrade_kraken_websocket.messages.listen import keep_messages_only

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
    share()
)
messages.subscribe(debug_observer('ALL MESSAGES'))
# Subscribe to multiple channels only when socket connects
ready = socket_connection.pipe(
    filter_new_socket_only(),
    share()
)
# subscribe_to_channel gives an observable with only the messages from that channel
ready.pipe(
    subscribe_to_channel(messages, channel=CHANNEL_TICKER, pair='USDT/USD')
).subscribe(
    info_observer('TICKER USDT')
)

# Disposing of the subscription to channel triggers sending the "unsubscribe" socket message. If needed to get around that, use share() or publish()
to_be_disposed = LogOnDisposeDisposable(
    ready.pipe(
        subscribe_to_channel(messages, channel=CHANNEL_SPREAD, pair='XRP/USD')
    ).subscribe(
        info_observer('SPREAD XRP')
    ),
    message='From here on, SPREAD XRP messages should not appear anymore'
)

sub = socket_connection.connect()

scheduler = TimeoutScheduler()

scheduler.schedule_relative(10, lambda *_: to_be_disposed.dispose())  # the "messages" will now show only "ticker", no more "spread"
scheduler.schedule_relative(60, lambda *_: sub.dispose()) # because all the subscriptions here are children of the socket connectable observable, everything will get cleaned up and websocket closed

import logging

from reactivex.operators import share
from reactivex.scheduler import TimeoutScheduler
from rich.logging import RichHandler

from bittrade_kraken_websocket.channels import subscribe_ticker
from bittrade_kraken_websocket.connection import public_websocket_connection
from bittrade_kraken_websocket.development import info_observer
from bittrade_kraken_websocket.messages.listen import keep_messages_only
from bittrade_kraken_websocket.operators import (
    filter_new_socket_only)

console = RichHandler()
console.setLevel(logging.DEBUG)
logger = logging.getLogger(
    'bittrade_kraken_websocket'
)
logger.setLevel(logging.DEBUG)
logger.addHandler(console)
socket_connection = public_websocket_connection(reconnect=True, shared=True)
messages = socket_connection.pipe(
    keep_messages_only(),
    share()
)
socket_connection.pipe(
    filter_new_socket_only(),
    subscribe_ticker("LTC/EUR", messages),
).subscribe(info_observer('TICKER'))

sub = socket_connection.connect()

scheduler = TimeoutScheduler()

scheduler.schedule_relative(60, lambda *_: sub.dispose())  # because all the subscriptions here are children of the socket connectable observable, everything will get cleaned up and websocket closed

while True:
    pass
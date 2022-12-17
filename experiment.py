import sys
import time
import logging
import orjson
import signal

from reactivex import operators

from bittrade_kraken_websocket.connection.public import public_websocket_connection, \
    public_websocket_connection_multicast
from rich import print as pretty_print
from concurrent.futures import ThreadPoolExecutor

from bittrade_kraken_websocket.connection.status import connected_socket
from bittrade_kraken_websocket.events.subscribe import subscribe_ticker
from bittrade_kraken_websocket.messages.heartbeat import ignore_heartbeat
from bittrade_kraken_websocket.messages.json import to_json
from bittrade_kraken_websocket.messages.listen import get_messages
from bittrade_kraken_websocket.messages.relevant import relevant_multicast

console = logging.StreamHandler(stream=sys.stdout)
console.setLevel(logging.DEBUG)
logger = logging.getLogger(
    'bittrade_kraken_websocket'
)
logger.setLevel(logging.DEBUG)
logger.addHandler(console)
socket_connection = public_websocket_connection_multicast()
connected = socket_connection.pipe(
    connected_socket()
)
messages = socket_connection.pipe(
    relevant_multicast()
)
ticker = connected.pipe(
    subscribe_ticker(messages, 'USDT/USD', 'ETH/USD')
)
ticker.subscribe(pretty_print, pretty_print, pretty_print)
messages.subscribe(
    pretty_print, pretty_print, pretty_print
)
sub = socket_connection.connect()
executor = ThreadPoolExecutor()
def wait_and_unsub():
    time.sleep(200)
    sub.dispose()

executor.submit(wait_and_unsub)
executor.shutdown(wait=False)
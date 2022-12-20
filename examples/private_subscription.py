import logging
from os import getenv

import reactivex
from reactivex import operators
from reactivex.operators import publish, share, take
from reactivex.scheduler import ThreadPoolScheduler
from rich.logging import RichHandler

from bittrade_kraken_websocket.channels import CHANNEL_OPEN_ORDERS
from bittrade_kraken_websocket.connection import private_websocket_connection, retry_with_backoff
from bittrade_kraken_rest.endpoints.private.get_websockets_token import get_websockets_token
from pathlib import Path
import urllib, hmac, base64, hashlib

from bittrade_kraken_websocket.connection.connection_operators import authenticated_socket
from bittrade_kraken_websocket.development import debug_observer, info_observer
from bittrade_kraken_websocket.events.subscribe import subscribe_to_channel
from bittrade_kraken_websocket.messages.listen import keep_messages_only, keep_status_only

console = RichHandler()
console.setLevel(logging.DEBUG)
logger = logging.getLogger(
    'bittrade_kraken_websocket'
)
logger.setLevel(logging.DEBUG)
logger.addHandler(console)


##### Don't use a library for this for security reasons; at most copy-paste this to your own code ####
# Code taken (with a minor change on non_null_data) from https://docs.kraken.com/rest/#section/Authentication/Headers-and-Signature
def generate_kraken_signature(urlpath, data, secret):
    non_null_data = {k: v for k, v in data.items() if v is not None}
    post_data = urllib.parse.urlencode(non_null_data)
    encoded = (str(data['nonce']) + post_data).encode()
    message = urlpath.encode() + hashlib.sha256(encoded).digest()
    mac = hmac.new(base64.b64decode(secret), message, hashlib.sha512)
    signature_digest = base64.b64encode(mac.digest())
    return signature_digest.decode()


def get_token():
    with get_websockets_token() as prep:
        prep.headers['API-Key'] = Path(
            './config_local/key').read_text()  # this reads key and secret from a gitignored folder at the root level; you could use env variables or other methods of loading your credentials
        prep.headers['API-Sign'] = generate_kraken_signature(prep.url, prep.data,
                                                             Path('./config_local/secret').read_text())
    return prep.response.get_result().token


##### END Write your own for security reasons ####

# Transform the above function into an observable
token_generator = reactivex.from_callable(get_token)

connection = private_websocket_connection(token_generator, json_messages=True).pipe(
    publish()
)
connection.pipe(
    keep_status_only()
).subscribe(debug_observer('Socket status'))
all_messages = connection.pipe(
    keep_messages_only(),
    share()
)
open_orders = connection.pipe(
    authenticated_socket(),
    subscribe_to_channel(all_messages, CHANNEL_OPEN_ORDERS)
)
open_orders.subscribe(info_observer('Own trades'))

pool_scheduler = ThreadPoolScheduler()

sub = connection.connect(pool_scheduler)


def stop():
    sub.dispose()


reactivex.interval(300).pipe(take(1)).subscribe(
    on_next=stop
)

import logging
from typing import Tuple

import reactivex
from bittrade_kraken_rest.models.private.get_websockets_token import GetWebsocketsTokenResult
from reactivex import operators
from reactivex.operators import publish, flat_map, do_action, ref_count
from reactivex.scheduler import ThreadPoolScheduler
from rich.logging import RichHandler
from websocket import WebSocketApp

from bittrade_kraken_websocket.connection import private_websocket_connection, retry_with_backoff
from bittrade_kraken_rest.endpoints.private.get_websockets_token import get_websockets_token
from bittrade_kraken_rest.models.request import RequestWithResponse
from pathlib import Path
from os import getenv
import urllib, hmac, base64, hashlib

from bittrade_kraken_websocket.connection.generic import WebsocketMessage
from bittrade_kraken_websocket.connection.private import AuthenticatedWebsocket
from bittrade_kraken_websocket.connection.status import WEBSOCKET_CLOSED, Status, WEBSOCKET_OPENED
from bittrade_kraken_websocket.development import debug_observer
from bittrade_kraken_websocket.messages.heartbeat import ignore_heartbeat
from bittrade_kraken_websocket.messages.listen import get_messages
from bittrade_kraken_websocket.messages.relevant import relevant, relevant_multicast

console = RichHandler()
console.setLevel(logging.DEBUG)
logger = logging.getLogger(
    'bittrade_kraken_websocket'
)
logger.setLevel(logging.DEBUG)
logger.addHandler(console)



##### Don't use a library for this for security reasons; at most copy-paste this to your own code ####
# Taken (with a minor change on non_null_data) from https://docs.kraken.com/rest/#section/Authentication/Headers-and-Signature
def generate_kraken_signature(urlpath, data, secret):
    non_null_data = {k: v for k, v in data.items() if v is not None}
    post_data = urllib.parse.urlencode(non_null_data)
    encoded = (str(data['nonce']) + post_data).encode()
    message = urlpath.encode() + hashlib.sha256(encoded).digest()
    mac = hmac.new(base64.b64decode(secret), message, hashlib.sha512)
    signature_digest = base64.b64encode(mac.digest())
    return signature_digest.decode()


def sign(request):
    request.headers['API-Key'] = Path('./config_local/key').read_text()
    request.headers['API-Sign'] = generate_kraken_signature(request.url, request.data, Path('./config_local/secret').read_text())
##### Write your own for security reasons ####

def enhance_websocket(message: WebsocketMessage) -> Tuple[AuthenticatedWebsocket, Status]:
    ws, status = message
    if status == WEBSOCKET_CLOSED:
        return (None, WEBSOCKET_CLOSED)
    prep: RequestWithResponse
    with get_websockets_token() as prep:
        sign(prep)
    result: GetWebsocketsTokenResult = prep.response.get_result()
    logger.debug('Received token %s', result)
    return (AuthenticatedWebsocket(ws, result.token), WEBSOCKET_OPENED,)


def authenticate():
    return operators.map(
        enhance_websocket
    )

connection = private_websocket_connection().pipe(
    publish()
)
connection.subscribe(debug_observer('Socket'))
authenticated = connection.pipe(
    authenticate(),
    # TODO change to a subscribe call
    do_action(lambda x: x[0].send_private({"event": "subscribe", "subscription": {"name": "openOrders"}})),
    publish(),
    ref_count()
)
pool_scheduler = ThreadPoolScheduler()

connection.pipe(
    ignore_heartbeat()
).subscribe(debug_observer('Messages'))
authenticated.pipe(
    relevant_multicast()
).subscribe(debug_observer('Authenticated'))

sub = connection.connect(pool_scheduler)

def stop():
    sub.dispose()

reactivex.interval(300).subscribe(
    on_next=stop
)

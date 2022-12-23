import logging
import signal

import reactivex
from reactivex.operators import publish, share, take
from reactivex.scheduler import ThreadPoolScheduler, TimeoutScheduler
from rich.logging import RichHandler

from bittrade_kraken_websocket.channels import CHANNEL_OPEN_ORDERS
from bittrade_kraken_websocket.connection import private_websocket_connection, retry_with_backoff
from bittrade_kraken_rest.endpoints.private.get_websockets_token import get_websockets_token
from pathlib import Path
import urllib, hmac, base64, hashlib

from bittrade_kraken_websocket.development import info_observer
from bittrade_kraken_websocket.channels.subscribe import subscribe_to_channel
from bittrade_kraken_websocket.messages.listen import keep_messages_only, filter_new_socket_only

console = RichHandler()
console.setLevel(logging.DEBUG)
logger = logging.getLogger(
    'bittrade_kraken_websocket'
)
logger.setLevel(logging.DEBUG)
logger.addHandler(console)

timeout_scheduler = TimeoutScheduler()

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

connection = private_websocket_connection(token_generator).pipe(
    retry_with_backoff(),
    publish()
)
# Uncomment this to see the status of the socket being emitted
# connection.pipe(
#     keep_status_only()
# ).subscribe(info_observer('Socket status'))
all_messages = connection.pipe(
    keep_messages_only(),
    share()
)
new_sockets = connection.pipe(
    filter_new_socket_only(),
    share()
)

# Uncomment this to see the socket reconnect in action (probably no backoff since kraken isn't actually disconnecting), followed by the re-subscription to the channels
# def force_close(socket: EnhancedWebsocket):
#     def close_me(*args):
#         socket.socket.close(status=1008)
#     timeout_scheduler.schedule_relative(10, close_me)
# connection.pipe(
#     map_socket_only(),
#     operators.do_action(on_next=force_close)
# ).subscribe()


open_orders = new_sockets.pipe(
    subscribe_to_channel(all_messages, CHANNEL_OPEN_ORDERS)
)
open_orders.subscribe(info_observer('[OPEN ORDERS]'))

# Uncomment this, (and comment out the original open_orders above) to see a fake sequence problem in subscription and the subsequent unsub/sub; you'll also need to place an order which should result in a sequence 3 at least - or you can change the code below to == 1
# def mess_up_sequence(x):
#     try:
#         if x[2]['sequence'] == 3:
#             x[2]['sequence'] = 5
#     except:
#         pass
#     return x
#
# open_orders = new_sockets.pipe(
#     subscribe_to_private_channel(
#         all_messages.pipe(
#             operators.map(mess_up_sequence)
#         )
#     , CHANNEL_OPEN_ORDERS)
# )
# open_orders.subscribe(info_observer('[MESSED UP ORDERS]'))


# Uncomment this to see additional socket connection in action
# own_trades = new_sockets.pipe(
#     subscribe_to_private_channel(all_messages, CHANNEL_OWN_TRADES)
# )
# own_trades.subscribe(debug_observer('[OWN TRADES]'))

pool_scheduler = ThreadPoolScheduler()

sub = connection.connect(pool_scheduler)

ongoing = True
def stop(*args):
    global ongoing
    ongoing = False
    sub.dispose()

signal.signal(
    signal.SIGINT, stop
)

# Uncomment this to stop the socket after 1 minute
reactivex.interval(600).pipe(take(1)).subscribe(
    on_next=stop
)

while ongoing:
    pass

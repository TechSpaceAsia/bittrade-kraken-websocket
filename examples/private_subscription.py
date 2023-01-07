import logging
import signal

from reactivex import operators, Observable
from reactivex.scheduler import ThreadPoolScheduler, TimeoutScheduler
from rich.logging import RichHandler

from bittrade_kraken_websocket import subscribe_open_orders, subscribe_own_trades

from bittrade_kraken_websocket.connection import (
    private_websocket_connection,
    retry_with_backoff,
)
from bittrade_kraken_rest import get_websockets_token_request, get_websockets_token_result, GetWebsocketsTokenResult
from pathlib import Path
import hmac, base64, hashlib
from urllib import parse
from bittrade_kraken_websocket import EnhancedWebsocket

from bittrade_kraken_websocket.development import info_observer
from bittrade_kraken_websocket.messages.listen import (
    keep_messages_only,
    filter_new_socket_only,
)

from requests.models import PreparedRequest

console = RichHandler()
console.setLevel(logging.DEBUG)
logger = logging.getLogger("bittrade_kraken_websocket")
logger.setLevel(logging.DEBUG)
logger.addHandler(console)

timeout_scheduler = TimeoutScheduler()

##### Don't use a library for this for security reasons; at most copy-paste this to your own code ####
# Code taken (with a minor change on non_null_data) from https://docs.kraken.com/rest/#section/Authentication/Headers-and-Signature
def generate_kraken_signature(urlpath, data, secret):
    post_data = parse.urlencode(data)
    encoded = (str(data["nonce"]) + post_data).encode()
    message = urlpath.encode() + hashlib.sha256(encoded).digest()
    mac = hmac.new(base64.b64decode(secret), message, hashlib.sha512)
    signature_digest = base64.b64encode(mac.digest())
    return signature_digest.decode()


def sign(x: tuple[PreparedRequest, str, dict]) -> PreparedRequest:
    request, url, data = x
    signature = generate_kraken_signature(url, data, Path("./config_local/secret").read_text())
    request = request.copy()
    request.headers["API-Key"] = Path(
        "./config_local/key"
    ).read_text()  # this reads key and secret from a gitignored folder at the root level; you could use env variables or other methods of loading your credentials
    request.headers["API-Sign"] = signature
    return request

def add_token(socket: EnhancedWebsocket) -> Observable[EnhancedWebsocket]:
    def set_token(result: GetWebsocketsTokenResult):
        socket.token = result.token
        return socket
    return get_websockets_token_request().pipe(
        operators.map(sign),
        get_websockets_token_result(),
        operators.map(set_token)
    )

##### END Write your own for security reasons ####

connection = private_websocket_connection(reconnect=True)
# Uncomment this to see the status of the socket being emitted
# connection.pipe(
#     keep_status_only()
# ).subscribe(info_observer('Socket status'))
all_messages = connection.pipe(keep_messages_only(), operators.share())
new_sockets = connection.pipe(
    filter_new_socket_only(),
    operators.flat_map(add_token),
    operators.share(),
)

# Uncomment this to see the socket reconnect in action (probably no backoff since kraken isn't actually disconnecting), followed by the re-subscription to the channels
# def force_close(socket: EnhancedWebsocket):
#     def close_me(*args):
#         socket.socket.close(status=1008)
#     timeout_scheduler.schedule_relative(10, close_me)
# new_sockets.pipe(
#     operators.do_action(on_next=force_close)
# ).subscribe()


open_orders = new_sockets.pipe(subscribe_open_orders(all_messages))
open_orders.subscribe(info_observer("[OPEN ORDERS]"))

# Uncomment this, (and comment out the original open_orders above) to see a fake sequence problem in subscription and the subsequent unsub/sub; 
# you'll also need to place an order which should result in a sequence 3 at least - or you can change the code below to x[2]['sequence'] == 1
# def mess_up_sequence(x):
#     try:
#         if x[2]['sequence'] == 3:
#             x[2]['sequence'] = 5
#     except:
#         pass
#     return x
#
# open_orders = new_sockets.pipe(
#     subscribe_open_orders(
#         all_messages.pipe(
#             operators.map(mess_up_sequence)
#         )
#     )
# )
# open_orders.subscribe(info_observer('[MESSED UP ORDERS]'))


# Uncomment this to see additional private feed subscription in action
# own_trades = new_sockets.pipe(subscribe_own_trades(all_messages))
# own_trades.subscribe(info_observer("[OWN TRADES]"))

sub = connection.connect()

ongoing = True


def stop(*args):
    global ongoing
    ongoing = False
    assert sub is not None
    sub.dispose()


signal.signal(signal.SIGINT, stop)

# Uncomment this to stop the socket after 1 minute
# time.sleep(60)
# stop()

while ongoing:
    pass

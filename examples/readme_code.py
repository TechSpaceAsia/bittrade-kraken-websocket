from bittrade_kraken_websocket import public_websocket_connection, subscribe_to_channel
from bittrade_kraken_websocket.operators import keep_messages_only, filter_new_socket_only

# Prepare connection - note, this is a ConnectableObservable, so it will only trigger connection when we call its connect method
socket_connection = public_websocket_connection()
# Prepare a feed with only "real" messages, dropping status update, heartbeat etc
messages = socket_connection.pipe(
    keep_messages_only(),
)
socket_connection.pipe(
    filter_new_socket_only(),
    subscribe_to_channel(messages, channel='ticker', pair='USDT/USD')
).subscribe(
    print, print, print  # you can do anything with the messages; this prints them out
)
socket_connection.connect()
Kraken Websocket
===

Features
---

- Reconnect with incremental backoff (per Kraken's recommendation)
- Automatically reset subscription for private feeds when sequence is out of whack
- request/response handlers e.g. `cancelAll` make websocket events feel like calling an API
- ... but provides more info than a simple request/response e.g. `addOrder` goes through each stage submitted->pending->open or canceled

Installing
---

`pip install bittrade-kraken-websocket` or `poetry add bittrade-kraken-websocket`

General considerations
---

### Observables/Reactivex

The whole library is build with [Reactivex](https://rxpy.readthedocs.io/en/latest/).

Though Observables seem complicated at first, they are the best way to handle and (synchronously) test complex situations that arise over time, like invalid sequence of messages or socket disconnection and backoff reconnects.

For simple use cases, they are also rather easy to use as shown in the [examples](./examples) folder or in the Getting Started below

### Concurrency

Internally the library uses threads.
For your main programme you don't have to worry about threads; you can block.

Getting started
---

### I just want to connect to the public feeds

```python
from bittrade_kraken_websocket import public_websocket_connection, subscribe_to_channel, ChannelName
from bittrade_kraken_websocket.operators import keep_messages_only, map_socket_only, ready_socket

# Prepare connection - note, this is a ConnectableObservable, so it will only trigger connection when we call its `connect` method
socket_connection = public_websocket_connection()
# Prepare a feed with only "real" messages, dropping status update, heartbeat etc
messages = socket_connection.pipe(
    keep_messages_only(),
)
socket_connection.pipe(
    ready_socket(),
    map_socket_only(),
    subscribe_to_channel(messages, channel=ChannelName.CHANNEL_TICKER, pair='USDT/USD')
).subscribe(
    print, print, print  # you can do anything with the messages; this prints them out
)
# We're all set, trigger connection
socket_connection.connect()
```


Logging
---

We use Python's standard logging.
You can modify what logs you see as follows:

```
logging.getLogger('bittrade_kraken_websocket').addHandler(logging.StreamHandler())
```

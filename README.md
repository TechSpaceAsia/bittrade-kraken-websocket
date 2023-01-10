# Kraken Websocket

[NOT RELEASED] This is very much a work in progress, despite being on pypi.
Most things might be wrongly documented; API **will** change

## Features

- Reconnect with incremental backoff (per Kraken's recommendation)
- Automatically reset subscription for private feeds when sequence is out of whack
- request/response factories e.g. `add_order_factory` make websocket events feel like calling an API
- ... but provides more info than a simple request/response; 
  for instance, `add_order` goes through each stage submitted->pending->open or canceled, 
  emitting a notification at each stage

## Installing

`pip install bittrade-kraken-websocket` or `poetry add bittrade-kraken-websocket`

## General considerations

### Observables/Reactivex

The whole library is build with [Reactivex](https://rxpy.readthedocs.io/en/latest/).

Though Observables seem complicated at first, they are the best way to handle - and (synchronously) test - complex situations that arise over time, like an invalid sequence of messages or socket disconnection and backoff reconnects.

For simple use cases, they are also rather easy to use as shown in the [examples](./examples) folder or in the Getting Started below

### Concurrency

Internally the library uses threads.
For your main program you don't have to worry about threads; you can block the main thread.

## Getting started

### Connect to the public feeds

```python
from bittrade_kraken_websocket import public_websocket_connection, subscribe_ticker
from bittrade_kraken_websocket.operators import keep_messages_only, filter_new_socket_only

# Prepare connection - note, this is a ConnectableObservable, so it will only trigger connection when we call its ``connect`` method
socket_connection = public_websocket_connection()
# Prepare a feed with only "real" messages, dropping things like status update, heartbeat, etc…
messages = socket_connection.pipe(
    keep_messages_only(),
)
socket_connection.pipe(
    filter_new_socket_only(),
    subscribe_ticker('USDT/USD', messages)
).subscribe(
    print, print, print  # you can do anything with the messages; here we simply print them out
)
socket_connection.connect()
```

_(This script is complete, it should run "as is")_


## Logging

We use Python's standard logging.
You can modify what logs you see as follows:

```
logging.getLogger('bittrade_kraken_websocket').addHandler(logging.StreamHandler())
```

## Private feeds

Similar to [bittrade-kraken-rest](https://github.com/TechSpaceAsia/bittrade-kraken-rest), this library attempts to get as little access to sensitive information as possible.

Currently, you need to set the token onto the `EnhancedWebsocket`; this means we have no access to your Api key and secret.
Since the token is connection based and can't be reused, this protects you as much as Kraken's current authentication method allows.

In the future we might even let you code your own `send_json` method instead.

See `examples/private_subscription.py` for an example of implementation

```python
new_sockets = connection.pipe(
    filter_new_socket_only(),
    operators.map(add_token),
    operators.share(),
)
```

## Examples

Most examples in the `examples` folder make use of the `development` module helpers and the rich logging. You will need to install the dependencies from the `rich` group to use them:

`poetry add bittrade_kraken_websocket -E rich`
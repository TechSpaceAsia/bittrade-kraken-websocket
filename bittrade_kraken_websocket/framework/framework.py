from typing import Callable, cast

from expression import Nothing, Option, Some

from bittrade_kraken_websocket.channels.open_orders import subscribe_open_orders
from bittrade_kraken_websocket.channels.own_trades import subscribe_own_trades
from bittrade_kraken_websocket.channels.ticker import subscribe_ticker
from bittrade_kraken_websocket.events.add_order import add_order_factory
from bittrade_kraken_websocket.events.cancel_order import cancel_order_factory
from bittrade_kraken_websocket.models.framework import FrameworkContext
from ccxt import kraken

from reactivex import Observable
from reactivex.subject import BehaviorSubject

from elm_framework_helpers.websockets.operators import connection_operators

from bittrade_kraken_websocket.connection.enhanced_websocket import EnhancedWebsocket
from bittrade_kraken_websocket.connection.private import private_websocket_connection
from bittrade_kraken_websocket.connection.public import public_websocket_connection

from reactivex import operators

from bittrade_kraken_websocket.messages.listen import filter_new_socket_only
from bittrade_kraken_websocket.channels import subscribe_ticker

def get_framework(
    add_token: Callable[[EnhancedWebsocket], Observable[EnhancedWebsocket]],
    load_markets=True,
) -> FrameworkContext:
    exchange = kraken()
    if load_markets:
        exchange.load_markets()

    public_bundles = public_websocket_connection(reconnect=True)
    public_sockets = public_bundles.pipe(
        connection_operators.keep_new_socket_only(),
        operators.share(),
    )

    public_socket_messages = public_bundles.pipe(
        connection_operators.keep_messages_only(),
        operators.share(),
    )

    private_bundles = private_websocket_connection(reconnect=True)
    private_sockets = private_bundles.pipe(
        filter_new_socket_only(),
        operators.flat_map(add_token),
        operators.share(),
    )
    private_socket_messages = private_bundles.pipe(
        connection_operators.keep_messages_only(),
        operators.share(),
    )
    socket_bs = BehaviorSubject(cast(Option[EnhancedWebsocket], Nothing))
    private_sockets.subscribe(lambda socket: socket_bs.on_next(Some(socket)))

    add_order = add_order_factory(socket_bs, private_socket_messages)
    cancel_order = cancel_order_factory(socket_bs, private_socket_messages)
    subscribe_ticker_factory = lambda pair: public_sockets.pipe(
        subscribe_ticker(pair, public_socket_messages)
    )
    subscribe_own_trades_factory = lambda: private_sockets.pipe(
        subscribe_own_trades(private_socket_messages)
    )
    subscribe_open_orders_factory = lambda: private_sockets.pipe(
        subscribe_open_orders(private_socket_messages)
    )

    return FrameworkContext(
        exchange=exchange,
        public_socket_bundles=public_bundles,
        public_sockets=public_sockets,
        public_socket_messages=public_socket_messages,
        private_socket_bundles=private_bundles,
        private_sockets=private_sockets,
        private_socket_messages=private_socket_messages,
        add_order=add_order,
        cancel_order=cancel_order,
        subscribe_ticker=subscribe_ticker_factory,
        subscribe_own_trades=subscribe_own_trades_factory,
        subscribe_open_orders=subscribe_open_orders_factory,
    )

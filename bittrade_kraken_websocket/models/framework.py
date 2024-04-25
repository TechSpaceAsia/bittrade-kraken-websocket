from dataclasses import dataclass
from typing import Any, Callable

from ccxt import kraken
from elm_framework_helpers.websockets import models
from reactivex import ConnectableObservable, Observable

from bittrade_kraken_websocket.channels.models.ticker import TickerPayload
from bittrade_kraken_websocket.channels.own_trades import OwnTradesPayload
from bittrade_kraken_websocket.connection.enhanced_websocket import EnhancedWebsocket
from bittrade_kraken_websocket.events.add_order import AddOrderRequest
from bittrade_kraken_websocket.events.cancel_order import CancelOrderRequest
from bittrade_kraken_websocket.events.models.order import Order


@dataclass
class FrameworkContext:
    exchange: kraken
    public_socket_bundles: ConnectableObservable[models.WebsocketBundle]
    public_sockets: Observable[EnhancedWebsocket]
    public_socket_messages: Observable[dict]
    private_socket_bundles: ConnectableObservable[models.WebsocketBundle]
    private_sockets: Observable[EnhancedWebsocket]
    private_socket_messages: Observable[dict]
    add_order: Callable[[AddOrderRequest], Observable[Order]]
    cancel_order: Callable[[CancelOrderRequest], Observable[Any]]
    subscribe_ticker: Callable[[str], Observable[TickerPayload]]
    subscribe_own_trades: Callable[[], Observable[OwnTradesPayload]]

import dataclasses
from decimal import Decimal
import functools
from logging import getLogger
from typing import Dict, List, TypedDict, Optional, Literal, Tuple

from reactivex import Observable, operators, throw
from reactivex.abc import ObserverBase, SchedulerBase
from reactivex.subject import BehaviorSubject
from reactivex.disposable import CompositeDisposable

from expression import Option, curry_flip

from bittrade_kraken_websocket.connection import EnhancedWebsocket
from bittrade_kraken_websocket.events.events import EventName
from bittrade_kraken_websocket.events.models.order import (
    Order,
    OrderType,
    OrderSide,
    OrderStatus,
    is_final_state,
)
from bittrade_kraken_websocket.events.ids import id_iterator
from bittrade_kraken_websocket.events.request_response import (
    wait_for_response,
    response_ok,
)

logger = getLogger(__name__)


class AddOrderError(Exception):
    pass


@dataclasses.dataclass
class AddOrderRequest:
    ordertype: OrderType
    type: OrderSide
    price: str
    volume: str
    pair: str
    oflags: str = ""
    price2: str = ""
    reqid: Optional[int] = None
    event: EventName = EventName.EVENT_ADD_ORDER
    userref: str = "1"


class AddOrderResponse(TypedDict):
    descr: str
    status: Literal["ok", "error"]
    txid: str


def _mapper_event_response_to_order(request: AddOrderRequest, message: Dict[str, str]):
    """
    {
      "descr": "buy 10.00000000 USDTUSD @ limit 0.9980",
      "event": "addOrderStatus",
      "reqid": 5,
      "status": "ok",
      "txid": "OXW22X-FYBXP-JQDBJT"
    }
    Error:
    {
      "errorMessage": "Unsupported field: 'refid' for the given msg type: add order",
      "event": "addOrderStatus",
      "pair": "USDT/USD",
      "status": "error"
    }
    """
    logger.info("[ORDER] Received response to add order request %s", message)

    return Order(
        order_id=message["txid"],
        status=OrderStatus.submitted,
        description=message["descr"],
        side=request.type,
        order_type=request.ordertype,
        price=Decimal(request.price),
    )


def map_response_to_order(request: AddOrderRequest):
    return operators.map(functools.partial(_mapper_event_response_to_order, request))


@curry_flip(1)
def order_related_messages_only(
    source: Observable[Dict | List], order_id: str
) -> Observable[Dict[str, str]]:
    def subscribe(observer: ObserverBase, scheduler: Optional[SchedulerBase] = None):
        def on_next(message):
            try:
                is_valid = message[1] == "openOrders" and order_id in message[0][0]
            except:
                pass
            else:
                if is_valid:
                    observer.on_next(message[0][0][order_id])

        return source.subscribe(
            on_next=on_next,
            on_error=observer.on_error,
            on_completed=observer.on_completed,
            scheduler=scheduler,
        )

    return Observable(subscribe)


def update_order(existing: Order, message: Dict) -> Order:
    updates = {
        "status": OrderStatus(message["status"]),
        "reference": message["userref"],
    }
    if "vol" in message:
        updates["volume"] = message["vol"]
    if "vol_exec" in message:
        updates["volume_executed"] = message["vol_exec"]
    if "open_tm" in message:
        updates["open_time"] = message["open_tm"]
    details = message.get("descr")
    if details and type(details) == dict:
        updates["price"] = message["descr"]["price"]
        updates["price2"] = message["descr"]["price2"]
    # Immutable version
    return dataclasses.replace(existing, **updates)


def create_order_lifecycle(
    x: Tuple[AddOrderRequest, EnhancedWebsocket], messages: Observable[Dict | List]
) -> Observable[Order]:
    request, connection = x

    def subscribe(observer: ObserverBase, scheduler: Optional[SchedulerBase] = None):
        # To be on the safe side, we start recording messages at this stage; note that there is currently no sign of the websocket sending messages in the wrong order though
        recorded_messages = messages.pipe(operators.replay())

        def initial_order_received(order: Order):
            order_id = order.order_id
            observer.on_next(order)
            return recorded_messages.pipe(
                order_related_messages_only(order_id),
                operators.scan(update_order, order),
                operators.take_while(
                    lambda o: not is_final_state(o.status), inclusive=True
                ),
            )

        obs = messages.pipe(
            wait_for_response(request.reqid, 5.0),
            response_ok(),
            map_response_to_order(request),
            operators.flat_map(initial_order_received),
        )
        connection.send_json(dataclasses.asdict(request))  # type: ignore
        return CompositeDisposable(
            obs.subscribe(observer, scheduler=scheduler), recorded_messages.connect()
        )

    return Observable(subscribe)


def add_order_factory(
    socket: BehaviorSubject[Option[EnhancedWebsocket]],
    messages: Observable[Dict | List],
):
    def add_order(request: AddOrderRequest) -> Observable[Order]:
        connection = socket.value
        if connection.is_none():
            return throw(ValueError("No socket"))
        current_connection = connection.value
        if not request.event:
            request.event = EventName.EVENT_ADD_ORDER
        if not request.reqid:
            request.reqid = next(id_iterator)

        return create_order_lifecycle((request, current_connection), messages)

    return add_order


__all__ = ["AddOrderError", "AddOrderRequest", "AddOrderResponse", "add_order_factory"]

import dataclasses
from decimal import Decimal
from logging import getLogger
from typing import Dict, List, TypedDict, Optional, Literal, Tuple
import typing

from reactivex import Observable, operators, throw
from reactivex.abc import ObserverBase, SchedulerBase
from reactivex.subject import BehaviorSubject
from reactivex.disposable import CompositeDisposable

from bittrade_kraken_websocket.connection import EnhancedWebsocket
from bittrade_kraken_websocket.events.events import EventName
from bittrade_kraken_websocket.events.models.order import Order, OrderType, OrderSide, OrderStatus, is_final_state
from bittrade_kraken_websocket.events.request_response import wait_for_response, response_ok

logger = getLogger(__name__)

class AddOrderError(Exception):
    pass


class AddOrderRequest(TypedDict):
    event: EventName
    ordertype: OrderType
    type: OrderSide
    price: str
    volume: str
    pair: str
    oflags: Optional[str]
    reqid: int


class AddOrderResponse(TypedDict):
    descr: str
    status: Literal["ok", "error"]
    txid: str


def _mapper_event_response_to_order(message: Dict):
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
    logger.info('[ORDER] Received response to add order request %s', message)

    return Order(
        order_id=message['txid'],
        status=OrderStatus.submitted,
        description=message['descr']
    )

def map_response_to_order():
    return operators.map(_mapper_event_response_to_order)

def order_related_messages_only(order_id: str):
    def _order_related_messages_only(source: Observable[Dict | List]) -> Observable[Dict]:
        def subscribe(observer: ObserverBase, scheduler: Optional[SchedulerBase] = None):
            def on_next(message):
                try:
                    is_valid = message[1] == "openOrders" and order_id in message[0][0]
                except:
                    pass
                else:
                    if is_valid:
                        observer.on_next(
                            message[0][0][order_id]
                        )
            return source.subscribe(
                on_next=on_next,
                on_error=observer.on_error,
                on_completed=observer.on_completed,
                scheduler=scheduler,
            )

        return Observable(subscribe)
    return _order_related_messages_only


def update_order(existing: Order, message: Dict) -> Order:
    updates = {
        'status': OrderStatus(message['status']),
        'reference': message['userref']
    }
    if 'vol' in message:
        updates['volume'] = Decimal(message['vol'])
    if 'vol_exec' in message:
        updates['volume_executed'] = Decimal(message['vol_exec'])
    if 'open_tm' in message:
        updates['open_time'] = message['open_tm']
    # Immutable version
    return dataclasses.replace(existing,
                               **updates
                               )

def create_order_lifecycle(x: Tuple[AddOrderRequest, EnhancedWebsocket], messages: Observable[Dict | List]) -> Observable[Order]:
    request, connection = x
    def subscribe(observer: ObserverBase, scheduler: Optional[SchedulerBase] = None):
        # To be on the safe side, we start recording messages at this stage; note that there is currently no sign of the websocket sending messages in the wrong order though
        recorded_messages = messages.pipe(
            operators.replay()
        )
        def initial_order_received(order: Order):
            order_id = order.order_id
            observer.on_next(order)
            return recorded_messages.pipe(
                order_related_messages_only(order_id),
                operators.scan(update_order, order),
                operators.take_while(
                    lambda o: not is_final_state(o.status)
                    , inclusive=True)
            )

        obs = messages.pipe(
            wait_for_response(request['reqid'], 5.0),
            response_ok(),
            map_response_to_order(),
            operators.flat_map(
                initial_order_received
            )
        )
        connection.send_json(request)  # type: ignore
        return CompositeDisposable(
            obs.subscribe(observer, scheduler=scheduler),
            recorded_messages.connect()
        )
    return Observable(subscribe)



def add_order_factory(socket: Observable[EnhancedWebsocket] | BehaviorSubject[Optional[EnhancedWebsocket]], messages: Observable[Dict | List]):
    # Keep track of the latest socket for easier sending
    connection: BehaviorSubject[Optional[EnhancedWebsocket]]
    if type(socket) != BehaviorSubject:
        # Note: for the time being this creates an infinite subscription, at least until socket is completed
        connection = BehaviorSubject(None)
        socket.subscribe(connection)
    else:
        connection = typing.cast(BehaviorSubject[Optional[EnhancedWebsocket]], socket)
    
    def add_order(request: AddOrderRequest) -> Observable[Order]:
        if not connection.value:
            return throw(ValueError('No socket'))
        current_connection = connection.value
        if 'event' not in request:
            request['event'] = EventName.EVENT_ADD_ORDER

        return create_order_lifecycle((request, current_connection), messages)

    return add_order


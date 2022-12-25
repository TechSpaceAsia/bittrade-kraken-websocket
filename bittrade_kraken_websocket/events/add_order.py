import dataclasses
from decimal import Decimal
from logging import getLogger
from typing import Dict, List, TypedDict, Optional, Literal, Tuple

from reactivex import Observable, operators, Observer
from reactivex.abc import ObserverBase, SchedulerBase
from reactivex.subject import BehaviorSubject

from bittrade_kraken_websocket.connection import EnhancedWebsocket
from bittrade_kraken_websocket.events.models.order import Order, OrderType, OrderSide, OrderStatus, is_final_state
from bittrade_kraken_websocket.events.request_response import wait_for_response, response_ok

logger = getLogger(__name__)

class AddOrderError(Exception):
    pass

class AddOrderRequest(TypedDict):
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
    def _order_related_messages_only(source) -> Observable[Dict]:
        def subscribe(observer: Observer, scheduler=None):
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
        'status': message['status'],
        'user_reference': message['userref']
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
    def subscribe(observer: ObserverBase, scheduler: Optional[SchedulerBase] = None) -> Observable[Order]:
        def initial_order_received(order: Order):
            order_id = order.order_id
            observer.on_next(order)
            messages.pipe(
                order_related_messages_only(order_id),
                operators.scan(update_order, order),
                operators.take_while(
                    lambda o: is_final_state(o.status)
                    , inclusive=True)
            ).subscribe(observer, scheduler=scheduler)
        messages.pipe(
            wait_for_response(request['reqid']),
            response_ok(),
            map_response_to_order(),
        ).subscribe(
            on_next=initial_order_received, scheduler=scheduler
        )
        connection.send_json(request)
    return Observable(subscribe)



def add_order_factory(sender: Observable[AddOrderRequest], socket: Observable[EnhancedWebsocket], messages: Observable[Dict | List]):
    def partial_create_order_lifecycle(x: Tuple[AddOrderRequest, EnhancedWebsocket]):
        return create_order_lifecycle(x, messages)

    return sender.pipe(
        operators.with_latest_from(socket)
    ).subscribe(
        on_next=partial_create_order_lifecycle
    )


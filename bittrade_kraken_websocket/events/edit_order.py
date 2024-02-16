import dataclasses
from logging import getLogger
from typing import Any, Dict, List, TypedDict, Optional, Literal, Tuple
import typing

from reactivex import Observable, operators, throw
from reactivex.abc import ObserverBase, SchedulerBase
from reactivex.subject import BehaviorSubject
from reactivex.disposable import CompositeDisposable
from expression import Option

from bittrade_kraken_websocket.connection import EnhancedWebsocket
from bittrade_kraken_websocket.events.events import EventName
from bittrade_kraken_websocket.events.models.order import (
    Order,
)
from bittrade_kraken_websocket.events.ids import id_iterator
from bittrade_kraken_websocket.events.request_response import (
    wait_for_response,
    response_ok,
)

logger = getLogger(__name__)


@dataclasses.dataclass
class EditOrderRequest:
    orderid: str
    pair: str
    volume: str
    price2: Optional[str] = ""
    reqid: Optional[int] = None
    price: Optional[str] = None
    oflags: Optional[str] = ""
    newuserref: Optional[str] = ""
    validate: Optional[str] = ""
    event: EventName = EventName.EVENT_EDIT_ORDER


class EditOrderResponse(TypedDict):
    txid: str # new order id
    originaltxid: str
    reqid: str
    status: Literal["ok", "error"]
    descr: str
    errorMessage: str


def edit_order_lifecycle(
    x: Tuple[EditOrderRequest, EnhancedWebsocket], messages: Observable[Dict | List]
) -> Observable[EditOrderResponse]:
    request, connection = x

    def subscribe(observer: ObserverBase, scheduler: Optional[SchedulerBase] = None):
        # To be on the safe side, we start recording messages at this stage; note that there is currently no sign of the websocket sending messages in the wrong order though
        recorded_messages = messages.pipe(operators.replay())
        sub = recorded_messages.connect()
        obs = messages.pipe(
            wait_for_response(request.reqid, 30.0),
            response_ok(),
        )
        connection.send_json(dataclasses.asdict(request, dict_factory=lambda x: {k: v for (k, v) in x if v is not None and v != ""}))  # type: ignore
        return CompositeDisposable(
            obs.subscribe(observer, scheduler=scheduler), 
            sub
        )

    return Observable(subscribe)


def edit_order_factory(
    socket: BehaviorSubject[Option[EnhancedWebsocket]],
    messages: Observable[Dict | List],
):
    def edit_order(request: EditOrderRequest) -> Observable[Any]:
        connection = socket.value
        if connection.is_none():
            return throw(ValueError("No socket"))
        current_connection = connection.value
        if not request.event:
            request.event = EventName.EVENT_EDIT_ORDER
        if not request.reqid:
            request.reqid = next(id_iterator)

        return edit_order_lifecycle((request, current_connection), messages)

    return edit_order



__all__ = [
    "EditOrderRequest",
    "EditOrderResponse",
    "edit_order_factory",
]

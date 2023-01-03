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


class CancelOrderError(Exception):
    pass


@dataclasses.dataclass
class CancelOrderRequest:
    txid: List[str]
    reqid: Optional[int] = None
    event: EventName = EventName.EVENT_CANCEL_ORDER


class CancelOrderResponse(TypedDict):
    descr: str
    status: Literal["ok", "error"]
    txid: str
    errorMessage: str


def cancel_order_lifecycle(
    x: Tuple[CancelOrderRequest, EnhancedWebsocket], messages: Observable[Dict | List]
) -> Observable[CancelOrderResponse]:
    request, connection = x

    def subscribe(observer: ObserverBase, scheduler: Optional[SchedulerBase] = None):
        # To be on the safe side, we start recording messages at this stage; note that there is currently no sign of the websocket sending messages in the wrong order though
        recorded_messages = messages.pipe(operators.replay())
        sub = recorded_messages.connect()
        obs = messages.pipe(
            wait_for_response(request.reqid, 5.0),
            response_ok(),
        )
        connection.send_json(dataclasses.asdict(request))  # type: ignore
        return CompositeDisposable(obs.subscribe(observer, scheduler=scheduler), sub)

    return Observable(subscribe)


def cancel_order_factory(
    socket: BehaviorSubject[Option[EnhancedWebsocket]],
    messages: Observable[Dict | List],
):
    def cancel_order(request: CancelOrderRequest) -> Observable[Any]:
        connection = socket.value
        if connection.is_none():
            return throw(ValueError("No socket"))
        current_connection = connection.value
        if not request.event:
            request.event = EventName.EVENT_CANCEL_ORDER
        if not request.reqid:
            request.reqid = next(id_iterator)

        return cancel_order_lifecycle((request, current_connection), messages)

    return cancel_order


__all__ = [
    "CancelOrderError",
    "CancelOrderRequest",
    "CancelOrderResponse",
    "cancel_order_factory",
]

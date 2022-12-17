import time
from typing import Callable, Dict, List, Optional

import reactivex
from reactivex import Observable, operators, Observer

from bittrade_kraken_websocket.events.events import EventType, EVENT_SUBSCRIBE

from logging import getLogger

logger = getLogger(__name__)

EventCaller = Callable[[Dict, int], Observable]


def request_response(sender: Observer[Dict], messages: Observable[Dict | List], timeout: Observable,
                     event_type: Optional[EventType] = None, raise_on_status: bool = False) -> EventCaller:
    def trigger_event(value: Dict, reqid: int = 0):
        message_id = reqid or int(1e6 * time.time())
        is_subscription_request = event_type == EVENT_SUBSCRIBE

        status_event_type = ''
        if event_type:
            status_event_type = f'{"subscription" if is_subscription_request else event_type}Status'

        def correct_id(message: Dict | List):
            try:
                if status_event_type and message['event'] != status_event_type:
                    return False
                if is_subscription_request:
                    return message['subscription']['name'] == value['subscription']['name']
                return message['reqid'] == message_id
            except:  # anything goes wrong, just pass
                return False

        def subscribe(observer: Observer, scheduler=None):
            merged = reactivex.merge(
                messages.pipe(
                    operators.filter(correct_id)
                ),
                timeout.pipe(
                    operators.flat_map(reactivex.throw(TimeoutError())),
                )
            ).pipe(
                operators.take(1 if not is_subscription_request else len(value['pair']))
            )
            if raise_on_status:
                merged = merged.pipe(
                    response_ok()
                )
            value['reqid'] = message_id
            sender.on_next(value)
            return merged.subscribe(observer, scheduler=scheduler)

        return Observable(subscribe)

    return trigger_event


class RequestResponseError(Exception):
    pass


def _response_ok(response):
    """
    Example from kraken:
    {
  "errorMessage": "Unsupported field: 'refid' for the given msg type: add order",
  "event": "addOrderStatus",
  "pair": "USDT/USD",
  "status": "error"
}
or {
  "descr": "buy 10.00000000 USDTUSD @ limit 0.9980",
  "event": "addOrderStatus",
  "reqid": 5,
  "status": "ok",
  "txid": "OXW22X-FYBXP-JQDBJT"
}
    """
    try:
        if response['status'] == 'error':
            raise RequestResponseError(response['errorMessage'])
        elif response['status'] == 'ok':
            return response
        raise RequestResponseError('Unknown status')
    except KeyError:
        raise Exception('Unknown response type')


def response_ok():
    return operators.map(_response_ok)

import functools
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

        taker = operators.take(1)
        if is_subscription_request:
            sub_channels = list(value['pair'])
            taker = operators.take_while(lambda _x: len(sub_channels) > 0, inclusive=True)

        def correct_id(message: Dict | List):
            try:
                if status_event_type and message['event'] != status_event_type:
                    return False
                if is_subscription_request:
                    sub = message['subscription']
                    return sub['name'] == value['subscription']['name'] and message['pair'] in sub_channels
                return message['reqid'] == message_id
            except:  # anything goes wrong, just pass
                return False

        def subscribe(observer: Observer, scheduler=None):
            filter_operators = [operators.filter(correct_id),]
            if is_subscription_request:
                filter_operators.append(
                    operators.do_action(on_next=lambda x: sub_channels.remove(x['pair']))
                )

            merged = reactivex.merge(
                messages.pipe(
                    *filter_operators
                ),
                timeout.pipe(
                    operators.flat_map(reactivex.throw(TimeoutError())),
                )
            ).pipe(
                taker
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


def _response_ok(response, good_status="ok", bad_status="error"):
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
        if response['status'] == bad_status:
            raise RequestResponseError(response['errorMessage'])
        elif response['status'] == good_status:
            return response
        raise RequestResponseError('Unknown status')
    except KeyError:
        raise Exception('Unknown response type')


def response_ok(good_status="ok", bad_status="error"):
    return operators.map(lambda response: _response_ok(response, good_status, bad_status))

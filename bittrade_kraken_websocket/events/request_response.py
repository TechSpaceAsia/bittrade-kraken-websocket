import functools
import time
from typing import Callable, Dict, List, Optional

import reactivex
from reactivex import Observable, operators, Observer, compose
from reactivex.operators import do_action

from bittrade_kraken_websocket.events.events import EventType, EVENT_SUBSCRIBE

from logging import getLogger

logger = getLogger(__name__)

EventCaller = Callable[[Dict, int], Observable]

def wait_for_response(is_match: Callable, timeout: Observable):
    return compose(
        operators.merge(
            timeout.pipe(
                operators.flat_map(reactivex.throw(TimeoutError()))
            )
        ),
        operators.filter(is_match),
        operators.take(1)
    )


def request_response(sender: Observer[Dict], messages: Observable[Dict | List], timeout: Observable,
                     event_type: Optional[EventType] = None, raise_on_status: bool = False) -> EventCaller:
    def trigger_event(value: Dict, reqid: int = 0):
        message_id = reqid or int(1e6 * time.time())
        """Subscription to channels such as ticker are a bit special so prepare to handle them"""
        is_subscription_request = event_type == EVENT_SUBSCRIBE

        status_event_type = ''
        if event_type:
            """Name of response 'event' value is <your event e.g.addOrder>Status unless it's a subscription then it's subscriptionStatus"""
            status_event_type = f'{"subscription" if is_subscription_request else event_type}Status'

        """
        Single requests like addOrder will only send one item back.
        Channels subscription can on the other hand have multiple "pairs" and they then get acknowledged one at a time
        Also single requests have status: ok/error, channel subscriptions receive "subscribed" 
        """
        taker = operators.take(1)
        good_status = "ok"
        bad_status = "error"
        pending_pairs = []
        if is_subscription_request:
            pending_pairs = list(value.get('pair', []))
            taker = operators.take_while(lambda _x: len(pending_pairs) > 0, inclusive=True)
            good_status = "subscribed"

        def keep_correct_id(message: Dict | List):
            return is_correct_id(
                message, status_event_type, is_subscription_request, value, message_id, pending_pairs
            )

        def subscribe(observer: Observer, scheduler=None):
            filter_operators = [operators.filter(keep_correct_id), ]
            if is_subscription_request and pending_pairs:
                """This allows to wait for all pairs. pending_pairs basically contains pairs that have not yet received their 'subscribe' event"""
                filter_operators.append(
                    operators.do_action(on_next=lambda x: pending_pairs.remove(x['pair']))
                )

            merged = reactivex.merge(
                messages.pipe(
                    *filter_operators
                ),
                timeout.pipe(
                    operators.flat_map(reactivex.throw(TimeoutError())),
                )
            ).pipe(
                taker,
                do_action(on_completed=lambda: logger.info('Received response to request: %s id %s', value, message_id)),
            )
            if raise_on_status:
                merged = merged.pipe(
                    response_ok(good_status, bad_status)
                )
            value['reqid'] = message_id
            sender.on_next(value)
            return merged.subscribe(observer, scheduler=scheduler)

        return Observable(subscribe)

    return trigger_event


def build_match_checker(message: Dict):
    is_subscription_request = message['event'] == EVENT_SUBSCRIBE
    request_id = message.get('reqid')
    def matcher(message: Dict | List):
        if type(message) == list:
            return False
        if request_id:
            return message.get('reqid') == request_id


    return matcher

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


def is_correct_id(message: Dict | List, status_event_type: str, is_subscription_request: bool, value: Dict,
                  message_id: int, pending_pairs: List[str] = None):
    pending_pairs = pending_pairs or []
    try:
        if status_event_type and message['event'] != status_event_type:
            return False
        if is_subscription_request:
            sub = message['subscription']
            if sub['name'] != value['subscription']['name']:
                return False
            if 'pair' in message:
                return message['pair'] in pending_pairs
        return message['reqid'] == message_id
    except:  # anything goes wrong, just pass
        return False

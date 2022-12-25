import uuid
from typing import Callable, Dict, List, Optional, Any

from reactivex import Observable, operators, Observer, compose
from reactivex.abc import ObserverBase, SchedulerBase
from reactivex.operators import do_action, take

from logging import getLogger

logger = getLogger(__name__)

EventCaller = Callable[[Dict, int], Observable]


def wait_for_response(is_match: Callable[[Any], bool] | int, timeout: float) -> Callable[[Observable[Dict | List]], Observable[Dict]]:
    if type(is_match) == int:
        is_match = build_matcher(is_match)
    return compose(
        operators.filter(is_match),
        operators.do_action(on_next=lambda x: logger.debug('[SOCKET] Received matching message')),
        operators.take(1),
        operators.timeout(timeout),
    )


def request_response_factory(send_request: Callable[[int], None], id_generator: Observable[int],
                             messages: Observable[Dict],
                             timeout: float):
    """
    A factory to create an observable which behaves like a request/response:
    on subscribe it calls `send_request` with the id of the request as argument.
    when a message is found in the `messages` stream that matches the id, it is emitted and the observable then completes

    :param send_request: A function that should send out the request on subscribe. Typically, lambda request_id: websocket.send_json(...)
    :param id_generator: An observable sequence of ids. Defaults to ids.id_generator which provides a (pretty unique) uuid1 based int. `.run()` is used on it, so it will block the current thread
    :param messages: The observable sequence of socket messages. It must contain at least the event messages (so cannot be limited to channel messages)
    :param timeout: Timeout in seconds
    :return:
    """

    def subscribe(observer: ObserverBase, scheduler: Optional[SchedulerBase] = None):
        message_id = id_generator.pipe(take(1)).run()
        sub = messages.pipe(
            wait_for_response(message_id, timeout),
        ).subscribe(
            on_error=observer.on_error,
            on_next=observer.on_next,
            on_completed=observer.on_completed,
            scheduler=scheduler
        )
        send_request(message_id)
        return sub

    return Observable(subscribe)


def request_response(sender: Observer[Dict], messages: Observable[Dict | List], timeout: float) -> EventCaller:
    def send_event_message(request_message: Dict):
        if 'reqid' not in request_message:
            request_message['reqid'] = uuid.uuid4().int & (1 << 64) - 1

        def subscribe(observer: ObserverBase, scheduler: Optional[SchedulerBase] = None):
            merged = messages.pipe(
                wait_for_response(
                    build_matcher(request_message['reqid']), timeout
                )
            ).pipe(
                do_action(
                    on_completed=lambda: logger.info('Received response to request: %s', request_message),
                    on_error=lambda exc: logger.error('Failed to get response for request %s -> %s', request_message,
                                                      exc, exc_info=True)
                ),
            )

            sender.on_next(request_message)
            return merged.subscribe(observer, scheduler=scheduler)

        return Observable(subscribe)

    return send_event_message


def build_matcher(reqid: int):
    def matcher(message: Dict | List):
        return type(message) == dict and message.get('reqid') == reqid

    return matcher


class RequestResponseError(Exception):
    pass


def _response_ok(response: Dict, good_status="ok", bad_status="error"):
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


def response_ok(good_status="ok", bad_status="error") -> Callable[[Observable[Dict]], Observable[Dict]]:
    return operators.map(lambda response: _response_ok(response, good_status, bad_status))

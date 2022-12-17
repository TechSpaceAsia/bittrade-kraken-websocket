from reactivex import compose
from reactivex.operators import publish, ref_count

from bittrade_kraken_websocket.messages.heartbeat import ignore_heartbeat
from bittrade_kraken_websocket.messages.json import to_json
from bittrade_kraken_websocket.messages.listen import get_messages


def relevant():
    return compose(
        get_messages(),
        ignore_heartbeat(),
        to_json(),
    )


def relevant_multicast():
    return compose(
        relevant(),
        publish(),
        ref_count()
    )

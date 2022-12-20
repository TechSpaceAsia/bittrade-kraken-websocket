from unittest.mock import MagicMock

import reactivex
from reactivex import Observable

from bittrade_kraken_websocket.connection.enhanced_websocket import EnhancedWebsocket


def test_is_private():
    assert EnhancedWebsocket(None, token='abc').is_private
    assert EnhancedWebsocket(None, token_generator='NOT NULL').is_private
    assert not EnhancedWebsocket(None, token='').is_private

def test_gets_token_on_first_call_only():
    obs = MagicMock()
    obs.run.return_value = 'abc'
    socket = MagicMock()
    ws = EnhancedWebsocket(socket, token_generator=obs)
    ws.send_json({})
    assert ws.token == 'abc'
    ws.send_json({})
    assert obs.run.call_count == 1
    assert socket.send.call_count == 2

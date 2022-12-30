from unittest.mock import MagicMock

import reactivex
from reactivex import Observable

from bittrade_kraken_websocket.connection.enhanced_websocket import EnhancedWebsocket


def test_is_private():
    assert EnhancedWebsocket(None, token='abc').is_private # type: ignore
    assert not EnhancedWebsocket(None, token='').is_private # type: ignore


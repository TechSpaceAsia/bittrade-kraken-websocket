from bittrade_kraken_websocket.connection.enhanced_websocket import EnhancedWebsocket
from bittrade_kraken_websocket.connection.status import Status
from reactivex import Observable
from reactivex.abc import SchedulerBase
from typing import Dict, List, Optional, Tuple, Union

WebsocketBundle = Tuple[EnhancedWebsocket, MessageTypes, Union[Status, Dict, List]]

def websocket_connection(private: bool = ..., scheduler: Optional[SchedulerBase] = ...) -> Observable[WebsocketBundle]: ...

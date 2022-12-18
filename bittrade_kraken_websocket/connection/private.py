from typing import Dict

from websocket import WebSocketApp
import orjson


class AuthenticatedWebsocket(WebSocketApp):
    token: str = ''

    def send_json(self, payload: Dict):
        return self.send(orjson.dumps(payload))

    def send_private(self, payload: Dict):
        # if subscription, token goes into that, otherwise goes to top level
        put_token_into = payload.get('subscription', payload)
        put_token_into['token'] = self.token
        return self.send_json(payload)

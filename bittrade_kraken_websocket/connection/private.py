from typing import Dict

from websocket import WebSocketApp
import orjson

from bittrade_kraken_websocket.connection.generic import websocket_connection


class AuthenticatedWebsocket:
    token: str = ''

    def __init__(self, underlying_socket, token):
        self.underlying_socket = underlying_socket
        self.token = token

    def send_json(self, payload: Dict):
        return self.underlying_socket.send(orjson.dumps(payload))

    def send_private(self, payload: Dict):
        # if subscription, token goes into that, otherwise goes to top level
        put_token_into = payload.get('subscription', payload)
        put_token_into['token'] = self.token
        return self.send_json(payload)


def private_websocket_connection():
    return websocket_connection(
        'wss://ws-auth.kraken.com'
    )

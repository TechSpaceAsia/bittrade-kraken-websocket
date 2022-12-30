from typing import TypeVar, Type, cast

from .models.message import PrivateMessage, PublicMessage

_T = TypeVar("_T")


def to_payload(message: PrivateMessage | PublicMessage, payload_type: Type[_T]) -> _T:
    return cast(_T, message[1])


def private_to_payload(message: PrivateMessage | PublicMessage,  payload_type: Type[_T]) -> _T:
    return cast(_T, message[0])

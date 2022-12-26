from typing import TypeVar, List

_T = TypeVar("_T")


def to_payload(message: List, klass: _T) -> _T:
    return message[1]


def private_to_payload(message: List):
    return message[0]

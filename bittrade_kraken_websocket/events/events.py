import enum


class EventName(enum.Enum):
    EVENT_ADD_ORDER = "addOrder"
    EVENT_CANCEL_ORDER = "cancelOrder"
    EVENT_EDIT_ORDER = "editOrder"
    EVENT_CANCEL_ALL = "cancelAll"
    EVENT_CANCEL_ALL_ORDERS_AFTER = "cancelAllOrdersAfter"
    EVENT_SUBSCRIBE = "subscribe"
    EVENT_UNSUBSCRIBE = "unsubscribe"


__all__ = [
    "EventName",
]

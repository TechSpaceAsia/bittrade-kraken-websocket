import dataclasses
from decimal import Decimal
from enum import Enum
from expression import Nothing, Option
from pydantic.dataclasses import dataclass

# For sending
class OrderType(str, Enum):
    market = "market"
    limit = "limit"
    stop_loss = "stop-loss"
    take_profit = "take-profit"
    stop_loss_limit = "stop-loss-limit"
    take_profit_limit = "take-profit-limit"
    settle_position = "settle-position"


class OrderSide(str, Enum):
    buy = "buy"
    sell = "sell"


class OrderStatus(str, Enum):
    canceled = "canceled"
    submitted = "submitted"
    pending = "pending"
    open = "open"
    blank = "blank"
    closed = "closed"


def is_final_state(status: OrderStatus):
    return status in [OrderStatus.canceled, OrderStatus.open]


default_zero = "0.00000000"


@dataclass
class Order:
    order_id: str
    status: OrderStatus
    side: OrderSide
    order_type: OrderType
    description: str
    reference: int = 0
    open_time: str = ""
    price: Decimal = Decimal("0")
    price2: Decimal = Decimal("0")
    volume: str = default_zero
    volume_executed: str = default_zero


__all__ = [
    "Order",
    "OrderStatus",
    "OrderType",
    "OrderSide",
]

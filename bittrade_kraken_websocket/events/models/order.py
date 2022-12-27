import dataclasses
from decimal import Decimal
from enum import Enum

# For sending
class OrderType(Enum):
    market = "market"
    limit = "limit"
    stop_loss = "stop-loss"
    take_profit = "take-profit"
    stop_loss_limit = "stop-loss-limit"
    take_profit_limit = "take-profit-limit"
    settle_position = "settle-position"


class OrderSide(Enum):
    buy = "buy"
    sell = "sell"


class OrderStatus(Enum):
    canceled  = 'canceled'
    submitted = 'submitted'
    pending = 'pending'
    open = 'open'
    blank = 'blank'


def is_final_state(status: OrderStatus):
    return status in [OrderStatus.canceled, OrderStatus.open]


default_zero = '0.00000000'

@dataclasses.dataclass
class Order:
    order_id: str
    status: OrderStatus
    description: str
    reference: int = 0
    open_time: str = ''
    price: str = default_zero
    price2: str = default_zero
    volume: str = default_zero
    volume_executed: str = default_zero


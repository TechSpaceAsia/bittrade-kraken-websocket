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


def is_final_state(status: str):
    return status in [OrderStatus.canceled.value, OrderStatus.open.value]


@dataclasses.dataclass
class Order:
    order_id: str
    status: OrderStatus
    description: str
    reference: str = ''
    open_time: str = ''
    volume = Decimal(0.0)
    volume_executed = Decimal(0.0)


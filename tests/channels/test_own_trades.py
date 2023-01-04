from decimal import Decimal
from bittrade_kraken_websocket.channels.own_trades import parse_own_trade, OwnTradesPayloadParsed
from bittrade_kraken_websocket.events import OrderSide, OrderType
from datetime import datetime

def test_parsing():
    out = parse_own_trade({
        "cost": "1000000.00000",
        "fee": "600.00000",
        "margin": "0.00000",
        "ordertxid": "TDLH43-DVQXD-2KHVYY",
        "ordertype": "limit",
        "pair": "XBT/EUR",
        "postxid": "OGTT3Y-C6I3P-XRI6HX",
        "price": "100000.00000",
        "time": "1560520332.914664",
        "type": "buy",
        "vol": "1000000000.00000000"
    })

    assert out.cost == Decimal("1000000.00000")
    assert out.type == OrderSide.buy
    assert out.ordertype == OrderType.limit
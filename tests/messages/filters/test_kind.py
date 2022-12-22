from bittrade_kraken_websocket.messages.filters.kind import _is_channel_message


def test_is_channel_message():
    func = _is_channel_message('channel name')
    assert not func({"c": 42})
    assert not func([])
    assert not func([1,2])
    assert not func([1,2, "anything"])
    assert not func([1,2, "anything", "more"])
    assert func([1,2, "channel name", "more"])
    func = _is_channel_message("ticker", "USDT/USD")
    assert not func({"c": 42, "name": "ticker"})
    assert not func(["ticker"])
    assert not func([1, "ticker"])
    assert not func([1, 2, "ticker"])
    assert not func([1, 2, "ohlc"]) # from kraken's messages, it's always the second to last which is the channel
    assert func([1, "ticker", "USDT/USD"])
    assert func([1, 2, "ticker", "USDT/USD"])
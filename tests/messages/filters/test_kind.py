from bittrade_kraken_websocket.messages.filters.kind import _is_channel_message


def test_is_channel_message():
    func = _is_channel_message()
    assert not func({"c": 42})
    assert not func([])
    assert not func([1,2])
    assert not func([1,2, "anything"])
    assert func([1,2, "anything", "more"])
    func = _is_channel_message("ownTrades", "ohlc")
    assert not func({"c": 42, "name": "ohlc"})
    assert not func(["ohlc"])
    assert not func([1, "ohlc"])
    assert not func([1, 2, "anything"])
    assert not func([1, 2, "ohlc"]) # from kraken's messages, it's always the second to last which is the channel
    assert func([1, 2, "ohlc", "more"])
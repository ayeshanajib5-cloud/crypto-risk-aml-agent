from producer.websocket_producer import transform_trade


def test_transform_trade_normalizes_binance_combined_stream_event():
    raw_event = {
        "stream": "btcusdt@trade",
        "data": {
            "e": "trade",
            "E": 1710000000000,
            "s": "BTCUSDT",
            "t": 12345,
            "p": "67500.50",
            "q": "0.250000",
            "b": 111,
            "a": 222,
            "T": 1710000000123,
            "m": True,
        },
    }

    trade = transform_trade(raw_event)

    assert trade["event_type"] == "trade"
    assert trade["symbol"] == "BTCUSDT"
    assert trade["trade_id"] == 12345
    assert trade["price"] == 67500.50
    assert trade["quantity"] == 0.25
    assert trade["notional_value"] == 16875.125
    assert trade["buyer_order_id"] == 111
    assert trade["seller_order_id"] == 222
    assert trade["trade_time_iso"].startswith("2024-03-09T16:00:00.123")
    assert trade["is_buyer_maker"] is True
    assert "ingested_at" in trade

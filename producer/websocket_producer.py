"""
WebSocket Producer: Binance Live Trade Stream → Kafka

Target role: Scotia Bank Market Surveillance, Coinbase/Kraken FDE
Why this matters: Every AML system starts with reliable, low-latency
ingestion of raw trade data. This is the heartbeat of the platform.

VWAP note: We capture price + quantity on every trade so Spark can
compute Volume Weighted Average Price downstream.
"""

import asyncio
import json
import logging
import os
import signal
import sys
from datetime import datetime, timezone

import websockets
from confluent_kafka import Producer, KafkaException

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("crypto.producer")

# ── Configuration (loaded from environment) ───────────────────────────────────
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
RAW_TRADES_TOPIC = os.getenv("RAW_TRADES_TOPIC", "raw-trades")

# Trading pairs to monitor — BTC and ETH cover 70%+ of suspicious volume
# In a real bank deployment you'd pull this list from a config service
SYMBOLS = [
    "btcusdt",   # Bitcoin / USDT
    "ethusdt",   # Ethereum / USDT
    "solusdt",   # Solana / USDT  (high wash-trading risk historically)
    "xrpusdt",   # XRP / USDT     (high FINTRAC alert volume)
]

# Binance combined stream URL — one WebSocket, multiple symbol feeds
# This is how production surveillance systems handle multi-asset monitoring
BINANCE_WS_URL = (
    "wss://stream.binance.com:9443/stream?streams="
    + "/".join(f"{s}@trade" for s in SYMBOLS)
)

# ── Kafka Producer Setup ──────────────────────────────────────────────────────
def build_producer() -> Producer:
    """
    Create a Kafka producer with delivery guarantees appropriate for
    financial data: acks=all means ALL in-sync replicas must confirm
    before we consider a message delivered. This is non-negotiable in
    AML systems — dropped trade events = compliance gaps = regulatory fines.
    """
    config = {
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "acks": "all",                    # strongest delivery guarantee
        "retries": 5,                     # retry on transient failures
        "retry.backoff.ms": 500,          # wait 500ms between retries
        "compression.type": "lz4",        # compress for throughput
        "linger.ms": 5,                   # batch messages for 5ms
        "batch.num.messages": 1000,       # max batch size
    }
    log.info("Connecting to Kafka at %s", KAFKA_BOOTSTRAP)
    return Producer(config)


def delivery_report(err, msg):
    """
    Callback fired after each Kafka message is acknowledged.
    In production this feeds a dead-letter queue if err is not None.
    For our platform it logs failures so we can audit gaps.
    """
    if err:
        log.error("Delivery FAILED | topic=%s | err=%s", msg.topic(), err)
    else:
        log.debug(
            "Delivered | topic=%s | partition=%d | offset=%d",
            msg.topic(), msg.partition(), msg.offset()
        )


# ── Message Transformation ────────────────────────────────────────────────────
def transform_trade(raw: dict) -> dict:
    """
    Normalize Binance trade event into our internal schema.

    Binance raw fields:
        e = event type, E = event time, s = symbol,
        t = trade ID, p = price, q = quantity,
        b = buyer order ID, a = seller order ID,
        T = trade time, m = is buyer market maker

    We add:
        ingested_at — our processing timestamp (for latency measurement)
        is_buyer_maker — True means the SELLER was the aggressor (they
                         hit the bid). Wash trading often shows symmetric
                         buyer/seller maker patterns.
    """
    data = raw.get("data", raw)  # handle combined stream wrapper
    return {
        "event_type": "trade",
        "symbol": data["s"],
        "trade_id": data["t"],
        "price": float(data["p"]),
        "quantity": float(data["q"]),
        "notional_value": float(data["p"]) * float(data["q"]),  # USD value
        "buyer_order_id": data.get("b"),
        "seller_order_id": data.get("a"),
        "trade_time_ms": data["T"],
        "trade_time_iso": datetime.fromtimestamp(
            data["T"] / 1000, tz=timezone.utc
        ).isoformat(),
        "is_buyer_maker": data["m"],  # True = sell aggressor
        "ingested_at": datetime.now(tz=timezone.utc).isoformat(),
    }


# ── Main WebSocket Loop ───────────────────────────────────────────────────────
async def stream_trades(producer: Producer):
    """
    Connect to Binance WebSocket and publish every trade to Kafka.
    Uses exponential backoff on reconnect — critical for 24/7 crypto
    markets where downtime means missed compliance events.
    """
    backoff = 1  # seconds

    while True:
        try:
            log.info("Opening WebSocket connection to Binance...")
            async with websockets.connect(
                BINANCE_WS_URL,
                ping_interval=20,    # keepalive every 20s
                ping_timeout=10,     # fail if no pong in 10s
                close_timeout=5,
            ) as ws:
                log.info("Connected. Streaming %d symbols.", len(SYMBOLS))
                backoff = 1  # reset backoff on successful connect

                async for raw_message in ws:
                    if isinstance(raw_message, bytes) or len(str(raw_message).strip()) <= 1:
                        continue
                    try:
                        raw = json.loads(raw_message)
                        trade = transform_trade(raw)

                        # Kafka key = symbol, so trades for the same
                        # asset always go to the same partition.
                        # This preserves ordering within a symbol —
                        # essential for VWAP time-series calculations.
                        producer.produce(
                            topic=RAW_TRADES_TOPIC,
                            key=trade["symbol"].encode(),
                            value=json.dumps(trade).encode(),
                            callback=delivery_report,
                        )
                        # Poll to trigger delivery callbacks
                        producer.poll(0)

                        log.info(
                            "▶ %s | price=%.4f | qty=%.6f | notional=$%.2f",
                            trade["symbol"],
                            trade["price"],
                            trade["quantity"],
                            trade["notional_value"],
                        )

                    except (KeyError, json.JSONDecodeError) as e:
                        log.warning("PARSE ERROR: %s | msg=%s", e, repr(raw_message)[:100])

        except websockets.exceptions.ConnectionClosed as e:
            log.warning("WebSocket closed (%s). Reconnecting in %ds...", e, backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)  # cap at 60s

        except Exception as e:
            log.error("Unexpected error: %s. Reconnecting in %ds...", e, backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)


# ── Entry Point ───────────────────────────────────────────────────────────────
async def main():
    producer = build_producer()

    # Graceful shutdown — flush Kafka buffer before exit
    # In production this prevents loss of in-flight messages
    def shutdown(sig, frame):
        log.info("Shutdown signal received. Flushing Kafka producer...")
        producer.flush(timeout=10)
        log.info("Producer flushed. Goodbye.")
        sys.exit(0)

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    await stream_trades(producer)


if __name__ == "__main__":
    asyncio.run(main())

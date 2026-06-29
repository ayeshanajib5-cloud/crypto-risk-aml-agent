"""Redis cache client for real-time risk signals."""

import os
import json
import redis
import logging

log = logging.getLogger("aml.redis")

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

def get_redis():
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

def get_latest_vwap(symbol: str):
    """Get latest VWAP for a symbol from Redis cache."""
    r = get_redis()
    val = r.get(f"vwap:{symbol.upper()}")
    return float(val) if val else None

def get_latest_zscore(symbol: str):
    """Get latest z-score for a symbol."""
    r = get_redis()
    val = r.get(f"zscore:{symbol.upper()}")
    return float(val) if val else None

def get_active_alert_count():
    """Get count of active AML alerts."""
    r = get_redis()
    val = r.get("alerts:active_count")
    return int(val) if val else 0

def set_risk_signal(symbol: str, vwap: float, zscore: float, is_anomalous: bool):
    """Cache latest risk signal for a symbol."""
    r = get_redis()
    r.setex(f"vwap:{symbol.upper()}", 300, str(vwap))
    r.setex(f"zscore:{symbol.upper()}", 300, str(zscore))
    if is_anomalous:
        r.incr("alerts:active_count")

def get_dashboard_summary():
    """Get real-time dashboard summary from Redis."""
    r = get_redis()
    symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT"]
    summary = {}
    for symbol in symbols:
        summary[symbol] = {
            "vwap":    r.get(f"vwap:{symbol}"),
            "zscore":  r.get(f"zscore:{symbol}"),
        }
    return {
        "signals":      summary,
        "active_alerts": get_active_alert_count(),
    }

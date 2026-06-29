"""Risk signals router."""

from fastapi import APIRouter, Query
from services.postgres_client import fetch_recent_risk_signals
from services.redis_cache import get_dashboard_summary, get_latest_vwap, get_latest_zscore

router = APIRouter(prefix="/risk", tags=["Risk Signals"])

@router.get("/dashboard")
async def dashboard():
    """Real-time risk dashboard — reads from Redis cache (<10ms latency)."""
    return get_dashboard_summary()

@router.get("/signals")
async def get_signals(
    symbol: str = Query(None, description="Filter by symbol e.g. BTCUSDT"),
    limit:  int = Query(50, le=500)
):
    """Historical risk signals from PostgreSQL."""
    return fetch_recent_risk_signals(symbol=symbol, limit=limit)

@router.get("/vwap/{symbol}")
async def get_vwap(symbol: str):
    """Get latest VWAP for a symbol."""
    vwap   = get_latest_vwap(symbol)
    zscore = get_latest_zscore(symbol)
    return {
        "symbol": symbol.upper(),
        "vwap":   vwap,
        "zscore": zscore,
        "source": "redis-cache"
    }

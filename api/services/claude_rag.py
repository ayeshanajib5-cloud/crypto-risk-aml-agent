"""
Claude RAG Service — AML Compliance Intelligence Layer

This is the $500k differentiator. A compliance analyst can ask natural
language questions grounded in live risk signals and trade data.

Example queries:
- "Show me all BTC anomalies in the last hour with z-score above 3"
- "How many FINTRAC-reportable events occurred today?"
- "What is the current VWAP for ETH and is it suspicious?"

Target: Scotia Bank compliance analysts, Coinbase/Kraken FDE roles
"""

import os
import json
import logging
from anthropic import Anthropic
from services.postgres_client import fetch_recent_risk_signals, fetch_aml_alerts
from services.redis_cache import get_dashboard_summary

log = logging.getLogger("aml.claude")

client = Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY", ""))

SYSTEM_PROMPT = """You are an expert AML (Anti-Money Laundering) compliance analyst 
assistant for a crypto trading surveillance platform. You have access to real-time 
market risk signals and compliance data.

Your expertise includes:
- VWAP (Volume Weighted Average Price) analysis
- Z-score based anomaly detection
- FINTRAC regulatory requirements (Canada)
- Wash trading and market manipulation detection
- Suspicious Transaction Report (STR) filing criteria

When answering questions:
1. Always ground your answers in the provided live data
2. Flag any FINTRAC-reportable concerns explicitly
3. Use precise financial terminology
4. Be concise but thorough
5. If z-scores exceed 3.0, flag as anomalous
6. If z-scores exceed 5.0, flag as critical and recommend STR review

You are serving compliance officers at a Canadian financial institution.
Regulatory accuracy is paramount."""

def build_context(symbol: str = None) -> str:
    """
    Build real-time context from live data sources.
    This is what makes this RAG — grounding Claude in actual data.
    """
    context_parts = []

    # ── Live dashboard from Redis ──────────────────────────────────────────────
    try:
        dashboard = get_dashboard_summary()
        context_parts.append("=== LIVE MARKET RISK SIGNALS (Redis Cache) ===")
        for sym, data in dashboard.get("signals", {}).items():
            vwap   = data.get("vwap",   "N/A")
            zscore = data.get("zscore", "N/A")
            context_parts.append(f"{sym}: VWAP={vwap}, Z-Score={zscore}")
        context_parts.append(
            f"Active Alerts: {dashboard.get('active_alerts', 0)}"
        )
    except Exception as e:
        log.warning("Redis context unavailable: %s", e)
        context_parts.append("Live signals: temporarily unavailable")

    # ── Recent risk signals from PostgreSQL ────────────────────────────────────
    try:
        signals = fetch_recent_risk_signals(symbol=symbol, limit=20)
        if signals:
            context_parts.append("\n=== RECENT RISK SIGNALS (PostgreSQL) ===")
            for s in signals[:10]:
                context_parts.append(
                    f"Symbol: {s.get('symbol')} | "
                    f"VWAP: {s.get('vwap')} | "
                    f"Price Z-Score: {s.get('price_zscore')} | "
                    f"Anomalous: {s.get('is_anomalous')} | "
                    f"Window: {s.get('window_start')} to {s.get('window_end')}"
                )
        else:
            context_parts.append("\nNo risk signals in database yet.")
    except Exception as e:
        log.warning("PostgreSQL signals unavailable: %s", e)

    # ── AML alerts ─────────────────────────────────────────────────────────────
    try:
        alerts = fetch_aml_alerts(limit=10)
        if alerts:
            context_parts.append("\n=== ACTIVE AML ALERTS ===")
            for a in alerts:
                context_parts.append(
                    f"Alert: {a.get('alert_type')} | "
                    f"Symbol: {a.get('symbol')} | "
                    f"Severity: {a.get('severity')} | "
                    f"FINTRAC Reportable: {a.get('fintrac_reportable')} | "
                    f"Status: {a.get('status')}"
                )
        else:
            context_parts.append("\nNo AML alerts currently active.")
    except Exception as e:
        log.warning("AML alerts unavailable: %s", e)

    return "\n".join(context_parts)


def query_compliance_agent(question: str, symbol: str = None) -> dict:
    """
    Main RAG query function.
    Builds live context, sends to Claude, returns grounded answer.
    """
    if not os.getenv("ANTHROPIC_API_KEY"):
        return {
            "answer": "Claude API key not configured. Set ANTHROPIC_API_KEY to enable AI compliance queries.",
            "context_used": False,
            "model": "none"
        }

    log.info("RAG query: %s", question)

    # Build grounded context from live data
    context = build_context(symbol=symbol)

    # Construct the grounded prompt
    user_message = f"""Here is the current live market risk and compliance data:

{context}

Based on this live data, please answer the following compliance question:

{question}"""

    try:
        response = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=1000,
            system=SYSTEM_PROMPT,
            messages=[
                {"role": "user", "content": user_message}
            ]
        )

        answer = response.content[0].text

        log.info("Claude response received (%d chars)", len(answer))

        return {
            "answer":       answer,
            "context_used": True,
            "model":        response.model,
            "usage": {
                "input_tokens":  response.usage.input_tokens,
                "output_tokens": response.usage.output_tokens,
            }
        }

    except Exception as e:
        log.error("Claude API error: %s", e)
        return {
            "answer":       f"Claude API error: {str(e)}",
            "context_used": False,
            "model":        "error"
        }

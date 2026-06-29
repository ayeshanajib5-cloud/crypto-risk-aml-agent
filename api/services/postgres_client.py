"""PostgreSQL client for compliance data."""

import os
import psycopg2
import psycopg2.extras
import logging

log = logging.getLogger("aml.postgres")

DB_CONFIG = {
    "host":     os.getenv("POSTGRES_HOST", "postgres"),
    "port":     int(os.getenv("POSTGRES_PORT", "5432")),
    "dbname":   os.getenv("POSTGRES_DB", "crypto_risk"),
    "user":     os.getenv("POSTGRES_USER", "riskadmin"),
    "password": os.getenv("POSTGRES_PASSWORD", "changeme_in_prod"),
}

def get_connection():
    return psycopg2.connect(**DB_CONFIG)

def fetch_recent_risk_signals(symbol: str = None, limit: int = 50):
    """Fetch recent risk signals, optionally filtered by symbol."""
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            if symbol:
                cur.execute("""
                    SELECT * FROM risk_signals
                    WHERE symbol = %s
                    ORDER BY computed_at DESC
                    LIMIT %s
                """, (symbol.upper(), limit))
            else:
                cur.execute("""
                    SELECT * FROM risk_signals
                    ORDER BY computed_at DESC
                    LIMIT %s
                """, (limit,))
            return [dict(row) for row in cur.fetchall()]
    finally:
        conn.close()

def fetch_aml_alerts(status: str = None, limit: int = 50):
    """Fetch AML alerts for compliance review."""
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            if status:
                cur.execute("""
                    SELECT * FROM aml_alerts
                    WHERE status = %s
                    ORDER BY created_at DESC
                    LIMIT %s
                """, (status.upper(), limit))
            else:
                cur.execute("""
                    SELECT * FROM aml_alerts
                    ORDER BY created_at DESC
                    LIMIT %s
                """, (limit,))
            return [dict(row) for row in cur.fetchall()]
    finally:
        conn.close()

def fetch_audit_log(limit: int = 100):
    """Fetch FINTRAC compliance audit log."""
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT * FROM compliance_audit_log
                ORDER BY created_at DESC
                LIMIT %s
            """, (limit,))
            return [dict(row) for row in cur.fetchall()]
    finally:
        conn.close()

def insert_audit_event(event_type: str, actor: str, action: str, metadata: dict = None):
    """Insert a FINTRAC audit event — immutable compliance record."""
    import json
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO compliance_audit_log
                (event_type, actor, action, metadata)
                VALUES (%s, %s, %s, %s)
            """, (
                event_type, actor, action,
                json.dumps(metadata) if metadata else None
            ))
        conn.commit()
    finally:
        conn.close()

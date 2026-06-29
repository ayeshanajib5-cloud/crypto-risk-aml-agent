-- ─────────────────────────────────────────────────────────────────────────────
-- Crypto Risk + AML Platform — Database Schema
--
-- FINTRAC context: Canada's Proceeds of Crime (Money Laundering) and
-- Terrorist Financing Act requires financial entities to report suspicious
-- transactions. Our schema captures everything needed for a Suspicious
-- Transaction Report (STR).
-- ─────────────────────────────────────────────────────────────────────────────

-- Raw trade events (mirror of what Kafka stores, for audit completeness)
CREATE TABLE IF NOT EXISTS raw_trades (
    id                  BIGSERIAL PRIMARY KEY,
    symbol              VARCHAR(20) NOT NULL,
    trade_id            BIGINT NOT NULL,
    price               NUMERIC(20, 8) NOT NULL,
    quantity            NUMERIC(20, 8) NOT NULL,
    notional_value      NUMERIC(20, 2) NOT NULL,
    is_buyer_maker      BOOLEAN NOT NULL,
    trade_time_ms       BIGINT NOT NULL,
    trade_time_iso      TIMESTAMPTZ NOT NULL,
    ingested_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(symbol, trade_id)
);

-- Risk signals computed by Spark (VWAP, z-score)
CREATE TABLE IF NOT EXISTS risk_signals (
    id                      BIGSERIAL PRIMARY KEY,
    symbol                  VARCHAR(20) NOT NULL,
    window_start            TIMESTAMPTZ NOT NULL,
    window_end              TIMESTAMPTZ NOT NULL,
    vwap                    NUMERIC(20, 8) NOT NULL,
    price_zscore            NUMERIC(10, 4),
    volume_zscore           NUMERIC(10, 4),
    trade_count             INTEGER NOT NULL,
    total_volume            NUMERIC(20, 8) NOT NULL,
    anomaly_score           NUMERIC(10, 6),
    is_anomalous            BOOLEAN DEFAULT FALSE,
    computed_at             TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- AML alerts — events flagged for compliance review
CREATE TABLE IF NOT EXISTS aml_alerts (
    id                          BIGSERIAL PRIMARY KEY,
    alert_id                    UUID NOT NULL DEFAULT gen_random_uuid(),
    symbol                      VARCHAR(20) NOT NULL,
    alert_type                  VARCHAR(50) NOT NULL,
    severity                    VARCHAR(20) NOT NULL,
    price_at_alert              NUMERIC(20, 8),
    volume_at_alert             NUMERIC(20, 8),
    notional_at_alert           NUMERIC(20, 2),
    zscore_at_alert             NUMERIC(10, 4),
    anomaly_score               NUMERIC(10, 6),
    regulatory_threshold_breached   BOOLEAN DEFAULT FALSE,
    fintrac_reportable              BOOLEAN DEFAULT FALSE,
    report_reference_id             VARCHAR(100),
    status                      VARCHAR(20) DEFAULT 'OPEN',
    reviewed_by                 VARCHAR(100),
    reviewed_at                 TIMESTAMPTZ,
    notes                       TEXT,
    created_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- FINTRAC audit trail — immutable log of all compliance decisions
CREATE TABLE IF NOT EXISTS compliance_audit_log (
    id              BIGSERIAL PRIMARY KEY,
    event_type      VARCHAR(50) NOT NULL,
    alert_id        UUID,
    actor           VARCHAR(100) NOT NULL,
    action          TEXT NOT NULL,
    metadata        JSONB,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Indexes for dashboard query performance
CREATE INDEX IF NOT EXISTS idx_risk_signals_symbol_window
    ON risk_signals(symbol, window_start DESC);

CREATE INDEX IF NOT EXISTS idx_aml_alerts_status
    ON aml_alerts(status, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_aml_alerts_fintrac
    ON aml_alerts(fintrac_reportable, created_at DESC)
    WHERE fintrac_reportable = TRUE;

INSERT INTO compliance_audit_log (event_type, actor, action, metadata)
VALUES (
    'SYSTEM_INIT',
    'system',
    'Database initialized with FINTRAC-aware schema',
    '{"version": "1.0", "environment": "local-dev"}'::jsonb
) ON CONFLICT DO NOTHING;

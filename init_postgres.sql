CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS position_snapshots (
    id             BIGSERIAL      PRIMARY KEY,
    book_id        TEXT           NOT NULL,
    symbol         TEXT           NOT NULL,
    net_quantity   NUMERIC(20, 8) NOT NULL DEFAULT 0,
    average_cost   NUMERIC(20, 8) NOT NULL DEFAULT 0,
    realized_pnl   NUMERIC(20, 2) NOT NULL DEFAULT 0,
    unrealized_pnl NUMERIC(20, 2) NOT NULL DEFAULT 0,
    last_px        NUMERIC(20, 8) NOT NULL DEFAULT 0,
    fill_count     INT            NOT NULL DEFAULT 0,
    updated_at     TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
    UNIQUE (book_id, symbol)
);

CREATE TABLE IF NOT EXISTS fix_executions (
    exec_id       TEXT           PRIMARY KEY,
    order_id      TEXT           NOT NULL,
    symbol        TEXT           NOT NULL,
    side          TEXT           NOT NULL,
    exec_type     TEXT           NOT NULL,
    last_px       NUMERIC(20, 8),
    last_qty      NUMERIC(20, 8),
    transact_time TIMESTAMPTZ    NOT NULL,
    venue         TEXT,
    sequence_no   BIGINT,
    book_id       TEXT,
    received_at   TIMESTAMPTZ    NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_fix_symbol_time ON fix_executions (symbol, transact_time DESC);
CREATE INDEX IF NOT EXISTS idx_fix_order       ON fix_executions (order_id);

CREATE TABLE IF NOT EXISTS micro_stall_events (
    id           BIGSERIAL      PRIMARY KEY,
    detected_at  TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
    layer        TEXT           NOT NULL,
    symbol       TEXT,
    latency_ms   NUMERIC(10, 3),
    threshold_ms NUMERIC(10, 3),
    details      JSONB
);

CREATE TABLE IF NOT EXISTS var_slo_audit (
    run_id          TEXT        PRIMARY KEY,
    run_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    total_elapsed_s NUMERIC(10, 3),
    slo_met         BOOLEAN,
    portfolio_var   NUMERIC(20, 2),
    traffic_light   TEXT
);

CREATE TABLE IF NOT EXISTS var_results_audit (
    id                  BIGSERIAL   PRIMARY KEY,
    dag_run_id          TEXT        NOT NULL,
    calculated_at       TIMESTAMPTZ NOT NULL,
    method              TEXT        NOT NULL,
    confidence          NUMERIC(5, 4),
    portfolio_var       NUMERIC(20, 2),
    stressed_var        NUMERIC(20, 2),
    diversification_ben NUMERIC(20, 2),
    lookback_days       INT,
    scenario_count      INT,
    calculation_ms      NUMERIC(10, 3),
    exceptions_60d      INT,
    kupiec_pvalue       NUMERIC(10, 6),
    traffic_light       TEXT,
    component_var       JSONB,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS pipeline_state (
    key        TEXT        PRIMARY KEY,
    value      TEXT        NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO pipeline_state (key, value)
VALUES ('circuit_breaker_state', 'closed')
ON CONFLICT (key) DO NOTHING;

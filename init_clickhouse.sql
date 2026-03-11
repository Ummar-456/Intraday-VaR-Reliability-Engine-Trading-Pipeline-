CREATE DATABASE IF NOT EXISTS financial_dre;

CREATE TABLE IF NOT EXISTS financial_dre.daily_pnl_history (
    date         Date,
    symbol       LowCardinality(String),
    rf_return    Float64,
    scenario_pnl Float64,
    actual_pnl   Float64
) ENGINE = ReplacingMergeTree()
ORDER BY (symbol, date)
PARTITION BY toYYYYMM(date)
TTL date + INTERVAL 2 YEAR;

CREATE TABLE IF NOT EXISTS financial_dre.var_results (
    calculated_at        DateTime64(3, 'UTC'),
    symbol               LowCardinality(String),
    symbol_var           Float64,
    portfolio_var        Float64,
    diversification_benefit Float64,
    calculation_ms       Float64,
    dag_run_id           String
) ENGINE = MergeTree()
ORDER BY (calculated_at, symbol)
PARTITION BY toYYYYMMDD(calculated_at)
TTL toDate(calculated_at) + INTERVAL 90 DAY;

CREATE TABLE IF NOT EXISTS financial_dre.trade_executions (
    received_at   DateTime64(3, 'UTC'),
    exec_id       String,
    order_id      String,
    symbol        LowCardinality(String),
    side          LowCardinality(String),
    last_px       Float64,
    last_qty      Float64,
    venue         LowCardinality(String),
    book_id       LowCardinality(String),
    sequence_no   UInt64
) ENGINE = MergeTree()
ORDER BY (received_at, symbol)
PARTITION BY toYYYYMMDD(received_at)
TTL toDate(received_at) + INTERVAL 30 DAY;

CREATE TABLE IF NOT EXISTS financial_dre.position_snapshots (
    snapshot_at    DateTime64(3, 'UTC'),
    book_id        LowCardinality(String),
    symbol         LowCardinality(String),
    net_quantity   Float64,
    average_cost   Float64,
    realized_pnl   Float64,
    unrealized_pnl Float64,
    last_px        Float64
) ENGINE = ReplacingMergeTree(snapshot_at)
ORDER BY (book_id, symbol);

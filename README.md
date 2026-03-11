# Financial DRE v2

Production-grade distributed reliability framework for intraday VaR calculation.

## Components

- `core/types.py`           — Domain types, Protocols, constants
- `core/position_manager.py`— FIX execution processing, VWAP, corporate actions
- `core/market_data.py`     — Correlated GBM market data (SVI vol surfaces)
- `core/greeks.py`          — Black-Scholes analytics (QuantLib optional)
- `core/historical_pnl.py`  — 250-day return window store (ClickHouse + in-memory)
- `core/var_engine.py`      — Historical simulation VaR + stressed VaR + Kupiec test
- `pipeline/var_dag_v2.py`  — Airflow DAG with 45s SLO
- `infra/position_store.py` — Redis + ClickHouse position store implementations
- `producer/fix_producer.py`— FIX drop copy simulation (50K msg/min)
- `consumer/kafka_consumer.py`— Kafka consumer with micro-stall detection
- `ebpf/tcp_profiler.py`    — Kernel-level DB latency profiling

## Quick start

```bash
docker compose up -d
```

Services:
- Grafana:    http://localhost:3000  (admin / dre_admin)
- Airflow:    http://localhost:8088  (admin / admin)
- Kafka UI:   http://localhost:8080
- Prometheus: http://localhost:9090

## Running tests

```bash
pytest tests/ -v
```

## Incident scenarios

```bash
python scripts/incident_simulator.py thundering-herd
python scripts/incident_simulator.py clickhouse-slow
python scripts/incident_simulator.py kupiec-breach
python scripts/incident_simulator.py sequence-gap
```

## SLO budget 

| Task             | Budget |
|------------------|--------|
| Health gate      |   2s   |
| Fetch positions  |   3s   |
| Market data      |   5s   |
| Greeks           |  10s   |
| VaR calculation  |  15s   |
| Write + distribute|  5s   |
| Buffer           |   5s   |
| **Total**        | **45s**|

## VaR methodology

Primary: Historical Simulation (250-day lookback, 99% confidence)  
Fallback: Delta-normal parametric (when history < 30 days)  
Regulatory: Stressed VaR (1.5x scaled scenarios, Basel 2.5)  
Backtesting: Kupiec POF test — p < 0.05 triggers Risk escalation

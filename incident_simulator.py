from __future__ import annotations

import random
import sys
import time
from datetime import datetime, timezone

import structlog

log = structlog.get_logger(__name__)


def thundering_herd() -> None:
    print("T+0s  : Rate spike 5x => 250K RPM equivalent => HTTP 429 storm")
    time.sleep(5)
    print("T+5s  : Exponential backoff activates on producers")
    time.sleep(10)
    print("T+15s : Circuit breaker OPENS after 5 consecutive failures")
    print("        Airflow DAG var_pipeline_v2 PAUSED via REST API")
    time.sleep(15)
    print("T+30s : Backoff plateau (delays: 0.5->1->2->4->8->16->32->60s cap)")
    time.sleep(15)
    print("T+45s : Circuit HALF_OPEN — probe request sent")
    time.sleep(15)
    print("T+60s : Rate limit restored, probe succeeds")
    print("        Circuit CLOSES")
    time.sleep(15)
    print("T+75s : Airflow DAG UNPAUSED — VaR pipeline resuming")
    print("        Full recovery confirmed")


def clickhouse_slowdown() -> None:
    print("T+0s  : ClickHouse TTFB degrading (>50ms per query)")
    print("        eBPF probe detects on port 9000")
    time.sleep(5)
    print("T+5s  : micro_stall_events_total{layer=db_write} incrementing")
    print("        Prometheus alert MicroStallDetected firing")
    time.sleep(10)
    print("T+15s : P&L fetch SLO at risk (10s of 15s budget consumed)")
    time.sleep(10)
    print("T+25s : VaR SLO breach — var_pipeline_slo_drift_s > 0")
    print("        PagerDuty alert: VaRSLOBreach")
    time.sleep(20)
    print("T+45s : ClickHouse recovered — TTFB back to <5ms")
    print("        Pipeline SLO restored")


def kupiec_breach() -> None:
    print("Simulating 12 VaR exceptions in 60 days...")
    print("Expected: ~0.6 exceptions at 99% confidence over 60 days")
    print("Observed: 12 => traffic_light=RED")
    print("Kupiec POF test p-value: {:.4f}".format(0.0003))
    print("ACTION: Escalate to Risk team. VaR model misspecified.")
    print("Possible causes:")
    print("  1. Lookback window too short (fat tail undersampled)")
    print("  2. Correlation spike not captured in historical data")
    print("  3. Position concentration in single risk factor")


def sequence_gap() -> None:
    print("T+0s  : FIX sequence gap detected: expected 1001, received 1008")
    print("        Gap = 7 messages (below halt threshold of 100)")
    print("        Position manager: WARNING logged, processing continues")
    time.sleep(3)
    print("T+3s  : Gap = 45 messages in next batch")
    print("        Still below halt threshold — WARNING")
    time.sleep(3)
    print("T+6s  : Gap = 112 messages — EXCEEDS halt threshold of 100")
    print("        SequenceGapError raised")
    print("        Position manager HALTED")
    print("        Alert: PositionIntegrityRisk firing")
    print("        ACTION: Manual reconciliation against OMS required")


SCENARIOS = {
    "thundering-herd": thundering_herd,
    "clickhouse-slow" : clickhouse_slowdown,
    "kupiec-breach"   : kupiec_breach,
    "sequence-gap"    : sequence_gap,
}

if __name__ == "__main__":
    scenario = sys.argv[1] if len(sys.argv) > 1 else "thundering-herd"
    fn       = SCENARIOS.get(scenario)
    if fn is None:
        print(f"Unknown scenario. Available: {', '.join(SCENARIOS)}")
        sys.exit(1)
    print(f"\nRunning scenario: {scenario}")
    print(f"Started at: {datetime.now(timezone.utc).isoformat()}")
    print()
    fn()
    print("\nScenario complete.")

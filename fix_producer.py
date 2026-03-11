from __future__ import annotations

import os
import random
import struct
import time
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal

import structlog
from confluent_kafka import Producer
from prometheus_client import Counter, Gauge, Histogram, start_http_server

log = structlog.get_logger(__name__)

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC     = os.environ.get("KAFKA_TOPIC", "fix-executions")
TARGET_RPM      = int(os.environ.get("TARGET_RATE_PER_MIN", "50000"))
BOOK_ID         = os.environ.get("BOOK_ID", "desk-1")
METRICS_PORT    = int(os.environ.get("METRICS_PORT", "8001"))

SYMBOLS = ["AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "TSLA", "JPM", "GS", "SPY", "QQQ"]
SPOTS   = {"AAPL": 189.5, "MSFT": 415.2, "NVDA": 875.0, "AMZN": 182.3,
           "GOOGL": 176.5, "TSLA": 242.8, "JPM": 198.4, "GS": 452.6,
           "SPY": 527.5, "QQQ": 451.2}

messages_sent    = Counter("fix_messages_total",          "Total FIX messages sent")
rate_gauge       = Gauge("fix_messages_per_minute",        "Current message rate")
circuit_gauge    = Gauge("circuit_breaker_open",           "Circuit breaker state")
retry_counter    = Counter("backoff_retry_attempts_total", "Backoff retry attempts")
latency_hist     = Histogram("fix_produce_latency_seconds","Produce latency",
                             buckets=[.001, .005, .01, .025, .05, .1, .25, .5, 1])


class TokenBucket:
    def __init__(self, rate_per_sec: float) -> None:
        self._rate     = rate_per_sec
        self._tokens   = rate_per_sec
        self._last     = time.monotonic()
        self._lock     = threading.Lock()

    def acquire(self) -> float:
        with self._lock:
            now              = time.monotonic()
            self._tokens    += (now - self._last) * self._rate
            self._tokens     = min(self._tokens, self._rate)
            self._last       = now
            if self._tokens >= 1.0:
                self._tokens -= 1.0
                return 0.0
            wait = (1.0 - self._tokens) / self._rate
            self._tokens = 0.0
            return wait


class CircuitBreaker:
    def __init__(self, threshold: int = 5, recovery_s: float = 30.0) -> None:
        self._threshold  = threshold
        self._recovery_s = recovery_s
        self._failures   = 0
        self._opened_at  : float | None = None
        self._state      = "closed"

    @property
    def is_open(self) -> bool:
        if self._state == "open":
            if time.monotonic() - self._opened_at >= self._recovery_s:
                self._state = "half_open"
                return False
            return True
        return False

    def record_success(self) -> None:
        self._failures = 0
        self._state    = "closed"
        circuit_gauge.set(0)

    def record_failure(self) -> None:
        self._failures += 1
        if self._failures >= self._threshold:
            self._state     = "open"
            self._opened_at = time.monotonic()
            circuit_gauge.set(1)
            log.error("circuit_breaker_opened", failures=self._failures)


def _jitter_backoff(attempt: int, base: float = 0.5, cap: float = 60.0) -> float:
    return random.uniform(0, min(cap, base * (2 ** attempt)))


def _encode_sbe_header(symbol: str, qty: int, px: float) -> bytes:
    sym_bytes = symbol.encode("ascii")[:8].ljust(8, b"\x00")
    return struct.pack(">HH8sId", 1001, 1, sym_bytes, qty, px)


def produce_executions() -> None:
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP, "linger.ms": 5, "batch.size": 65536})
    bucket   = TokenBucket(TARGET_RPM / 60)
    cb       = CircuitBreaker()
    seq_no   = 0

    start_http_server(METRICS_PORT)
    log.info("fix_producer_started", target_rpm=TARGET_RPM, topic=KAFKA_TOPIC)

    t_window = time.monotonic()
    sent_window = 0

    while True:
        wait = bucket.acquire()
        if wait > 0:
            time.sleep(wait)

        if cb.is_open:
            time.sleep(1.0)
            continue

        sym  = random.choice(SYMBOLS)
        px   = SPOTS[sym] * (1 + random.gauss(0, 0.0005))
        qty  = random.randint(10, 500)
        side = random.choice(["BUY", "SELL"])
        seq_no += 1

        payload = {
            "exec_id"      : f"E{seq_no:012d}",
            "order_id"     : f"O{random.randint(1, 100000):010d}",
            "cl_ord_id"    : f"CL{seq_no:012d}",
            "symbol"       : sym,
            "side"         : side,
            "exec_type"    : "FILL",
            "last_px"      : round(px, 4),
            "last_qty"     : str(qty),
            "cum_qty"      : str(qty),
            "avg_px"       : round(px, 4),
            "transact_time": datetime.now(timezone.utc).isoformat(),
            "venue"        : "NASDAQ",
            "sequence_no"  : seq_no,
            "book_id"      : BOOK_ID,
        }

        import json
        t0 = time.perf_counter()
        for attempt in range(8):
            try:
                producer.produce(
                    KAFKA_TOPIC,
                    key=sym.encode(),
                    value=json.dumps(payload).encode(),
                    headers={"sbe_header": _encode_sbe_header(sym, qty, px)},
                )
                producer.poll(0)
                cb.record_success()
                latency_hist.observe(time.perf_counter() - t0)
                messages_sent.inc()
                sent_window += 1
                break
            except Exception as e:
                cb.record_failure()
                retry_counter.inc()
                if attempt < 7:
                    time.sleep(_jitter_backoff(attempt))
                else:
                    log.error("produce_failed_max_retries", symbol=sym, error=str(e))

        now = time.monotonic()
        elapsed = now - t_window
        if elapsed >= 60.0:
            rate_gauge.set(sent_window / elapsed * 60)
            sent_window = 0
            t_window    = now


if __name__ == "__main__":
    produce_executions()

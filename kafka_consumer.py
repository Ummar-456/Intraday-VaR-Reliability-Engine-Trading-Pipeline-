from __future__ import annotations

import json
import os
import threading
import time
from datetime import datetime, timezone
from decimal import Decimal

import structlog
from confluent_kafka import Consumer, KafkaError
from prometheus_client import Counter, Gauge, Histogram, start_http_server

log = structlog.get_logger(__name__)

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC     = os.environ.get("KAFKA_TOPIC", "fix-executions")
KAFKA_GROUP     = os.environ.get("KAFKA_GROUP", "dre-consumer")
METRICS_PORT    = int(os.environ.get("METRICS_PORT", "8002"))
BOOK_ID         = os.environ.get("BOOK_ID", "desk-1")

consumer_lag     = Gauge("kafka_consumer_lag_messages",   "Consumer lag", ["partition"])
e2e_latency      = Histogram("trade_e2e_latency_seconds", "End-to-end trade latency",
                              buckets=[.001, .005, .01, .025, .05, .1, .25, .5, 1, 2])
messages_proc    = Counter("consumer_messages_processed_total", "Messages processed")
stall_counter    = Counter("micro_stall_events_total",          "Micro-stall events", ["layer"])
write_latency    = Histogram("clickhouse_write_latency_seconds","ClickHouse write latency",
                              buckets=[.005, .01, .025, .05, .1, .25, .5, 1])

_STALL_THRESHOLDS = {"db_write": 0.050, "position_update": 0.020, "kafka_poll": 0.005}


def _detect_stall(layer: str, elapsed_s: float) -> None:
    threshold = _STALL_THRESHOLDS.get(layer, 0.050)
    if elapsed_s > threshold:
        stall_counter.labels(layer=layer).inc()
        log.warning("micro_stall", layer=layer, elapsed_ms=round(elapsed_s * 1000, 2), threshold_ms=threshold * 1000)


def _clickhouse():
    import clickhouse_connect
    return clickhouse_connect.get_client(
        host=os.environ.get("CLICKHOUSE_HOST", "localhost"),
        username=os.environ.get("CLICKHOUSE_USER", "dre_user"),
        password=os.environ.get("CLICKHOUSE_PASS", "dre_pass"),
        database="financial_dre",
        connect_timeout=5,
        send_receive_timeout=10,
    )


def _position_store():
    import redis
    from infra.position_store import RedisPositionStore
    return RedisPositionStore(os.environ.get("REDIS_URL", "redis://localhost:6379/0"))


def _write_execution_batch(ch, batch: list[dict]) -> None:
    if not batch:
        return
    t0 = time.perf_counter()
    rows = [
        [
            datetime.fromisoformat(msg["transact_time"]),
            msg["exec_id"], msg["order_id"], msg["symbol"],
            msg["side"], float(msg["last_px"]), float(msg["last_qty"]),
            msg.get("venue", ""), msg.get("book_id", BOOK_ID),
            int(msg.get("sequence_no", 0)),
        ]
        for msg in batch
    ]
    ch.insert("trade_executions", rows, column_names=[
        "received_at", "exec_id", "order_id", "symbol",
        "side", "last_px", "last_qty", "venue", "book_id", "sequence_no",
    ])
    elapsed = time.perf_counter() - t0
    write_latency.observe(elapsed)
    _detect_stall("db_write", elapsed)


def run_consumer() -> None:
    from core.types import ExecType, FIXExecution, Side, SequenceNumber
    from core.position_manager import PositionManager

    consumer = Consumer({
        "bootstrap.servers"           : KAFKA_BOOTSTRAP,
        "group.id"                    : KAFKA_GROUP,
        "auto.offset.reset"           : "earliest",
        "enable.auto.commit"          : False,
        "max.poll.interval.ms"        : 300_000,
        "session.timeout.ms"          : 30_000,
        "fetch.max.bytes"             : 52_428_800,
    })
    consumer.subscribe([KAFKA_TOPIC])

    ch       = _clickhouse()
    store    = _position_store()
    pm       = PositionManager(store, BOOK_ID, gap_halt_threshold=100)
    batch    : list[dict] = []
    BATCH_SZ = 500
    FLUSH_S  = 1.0
    last_flush = time.monotonic()

    start_http_server(METRICS_PORT)
    log.info("consumer_started", topic=KAFKA_TOPIC, group=KAFKA_GROUP)

    while True:
        t_poll = time.perf_counter()
        msg    = consumer.poll(timeout=0.1)
        _detect_stall("kafka_poll", time.perf_counter() - t_poll)

        if msg is None:
            pass
        elif msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                log.error("kafka_error", error=msg.error())
        else:
            try:
                payload  = json.loads(msg.value().decode())
                recv_time = time.time()
                send_time = datetime.fromisoformat(payload["transact_time"]).timestamp()
                e2e_latency.observe(recv_time - send_time)

                exec_ = FIXExecution(
                    exec_id      = payload["exec_id"],
                    order_id     = payload["order_id"],
                    cl_ord_id    = payload.get("cl_ord_id", ""),
                    symbol       = payload["symbol"],
                    side         = Side(payload["side"]),
                    exec_type    = ExecType(payload.get("exec_type", "FILL")),
                    last_px      = float(payload["last_px"]),
                    last_qty     = Decimal(str(payload["last_qty"])),
                    cum_qty      = Decimal(str(payload.get("cum_qty", payload["last_qty"]))),
                    avg_px       = float(payload.get("avg_px", payload["last_px"])),
                    transact_time= datetime.fromisoformat(payload["transact_time"]),
                    venue        = payload.get("venue", ""),
                    sequence_no  = SequenceNumber(int(payload.get("sequence_no", 0))),
                )
                t_pos = time.perf_counter()
                pm.apply_execution(exec_)
                _detect_stall("position_update", time.perf_counter() - t_pos)

                batch.append(payload)
                messages_proc.inc()
            except Exception as e:
                log.error("message_processing_failed", error=str(e))

        now = time.monotonic()
        if len(batch) >= BATCH_SZ or (batch and now - last_flush >= FLUSH_S):
            _write_execution_batch(ch, batch)
            consumer.commit(asynchronous=False)
            batch      = []
            last_flush = now

        partitions = consumer.assignment()
        for p in partitions:
            try:
                lo, hi = consumer.get_watermark_offsets(p, timeout=0.5)
                pos    = consumer.position([p])
                lag    = hi - (pos[0].offset if pos[0].offset >= 0 else lo)
                consumer_lag.labels(partition=str(p.partition)).set(lag)
            except Exception:
                pass


if __name__ == "__main__":
    run_consumer()

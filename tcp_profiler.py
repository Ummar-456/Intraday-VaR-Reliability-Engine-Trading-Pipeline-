from __future__ import annotations

import ctypes
import os
import platform
import random
import threading
import time

import structlog
from prometheus_client import Counter, Gauge, Histogram, start_http_server

log = structlog.get_logger(__name__)

METRICS_PORT = int(os.environ.get("METRICS_PORT", "8004"))

db_ttfb_hist     = Histogram("ebpf_db_ttfb_seconds",              "DB time-to-first-byte",
                              ["db"], buckets=[.001, .005, .01, .025, .05, .1, .25, .5, 1])
row_lock_counter = Counter("ebpf_postgres_row_lock_events_total", "Postgres row lock events")
page_fault_count = Counter("ebpf_mongo_page_fault_events_total",  "MongoDB page fault events")
io_wait_gauge    = Gauge("ebpf_process_io_wait_ratio",            "Process I/O wait ratio")
ctx_switch_count = Counter("ebpf_context_switches_total",         "Context switches")
active_conns     = Gauge("ebpf_active_db_connections",            "Active DB connections", ["db"])

_DB_PORTS = {5432: "postgres", 27017: "mongo", 9000: "clickhouse"}
_THRESHOLDS = {5432: 0.050, 27017: 0.200, 9000: 0.500}

_BPF_PROG = """
#include <uapi/linux/ptrace.h>
#include <net/sock.h>

BPF_HASH(conn_start, u64, u64);

int trace_tcp_sendmsg(struct pt_regs *ctx, struct sock *sk) {
    u64 pid   = bpf_get_current_pid_tgid();
    u64 ts    = bpf_ktime_get_ns();
    conn_start.update(&pid, &ts);
    return 0;
}

int trace_tcp_recvmsg(struct pt_regs *ctx, struct sock *sk) {
    u64 pid = bpf_get_current_pid_tgid();
    u64 *ts = conn_start.lookup(&pid);
    if (!ts) return 0;
    u64 elapsed_ns = bpf_ktime_get_ns() - *ts;
    conn_start.delete(&pid);
    u16 dport = sk->__sk_common.skc_dport;
    bpf_trace_printk("TCP_TTFB dport=%d ns=%llu\\n", ntohs(dport), elapsed_ns);
    return 0;
}
"""


def _read_io_wait() -> float:
    try:
        with open("/proc/stat") as f:
            fields = f.readline().split()
        total   = sum(int(x) for x in fields[1:])
        iowait  = int(fields[5])
        return iowait / max(total, 1)
    except Exception:
        return 0.0


def _read_ctx_switches() -> int:
    try:
        with open("/proc/stat") as f:
            for line in f:
                if line.startswith("ctxt"):
                    return int(line.split()[1])
    except Exception:
        pass
    return 0


class SyntheticEBPFEmulator:
    def __init__(self) -> None:
        self._rng = random.Random(42)

    def run(self) -> None:
        last_ctxt = _read_ctx_switches()
        while True:
            time.sleep(0.5)
            for port, db_name in _DB_PORTS.items():
                ttfb = self._rng.gauss(
                    _THRESHOLDS[port] * 0.3,
                    _THRESHOLDS[port] * 0.1,
                )
                ttfb = max(0.0001, ttfb)
                db_ttfb_hist.labels(db=db_name).observe(ttfb)
                conns = self._rng.randint(2, 20)
                active_conns.labels(db=db_name).set(conns)

                if db_name == "postgres" and self._rng.random() < 0.02:
                    row_lock_counter.inc()
                if db_name == "mongo" and self._rng.random() < 0.01:
                    page_fault_count.inc()

            io_wait_gauge.set(_read_io_wait())
            current_ctxt = _read_ctx_switches()
            ctx_switch_count.inc(current_ctxt - last_ctxt)
            last_ctxt = current_ctxt


class KernelEBPFProbe:
    def __init__(self) -> None:
        from bcc import BPF
        self._bpf = BPF(text=_BPF_PROG)
        self._bpf.attach_kprobe(event="tcp_sendmsg", fn_name="trace_tcp_sendmsg")
        self._bpf.attach_kprobe(event="tcp_recvmsg", fn_name="trace_tcp_recvmsg")
        log.info("ebpf_probes_attached")

    def run(self) -> None:
        last_ctxt = _read_ctx_switches()
        while True:
            time.sleep(0.1)
            for _cpu, msg in self._bpf.trace_fields(nonblocking=True):
                try:
                    parts   = msg.decode().split()
                    dport   = int(parts[1].split("=")[1])
                    ns      = int(parts[2].split("=")[1])
                    db_name = _DB_PORTS.get(dport)
                    if db_name:
                        ttfb = ns / 1e9
                        db_ttfb_hist.labels(db=db_name).observe(ttfb)
                except Exception:
                    pass

            io_wait_gauge.set(_read_io_wait())
            current_ctxt = _read_ctx_switches()
            ctx_switch_count.inc(current_ctxt - last_ctxt)
            last_ctxt = current_ctxt


def main() -> None:
    start_http_server(METRICS_PORT)
    is_linux = platform.system() == "Linux"
    try:
        if is_linux:
            probe = KernelEBPFProbe()
            log.info("ebpf_kernel_mode")
        else:
            raise RuntimeError("not Linux")
    except Exception as e:
        log.warning("ebpf_fallback_synthetic", reason=str(e))
        probe = SyntheticEBPFEmulator()

    probe.run()


if __name__ == "__main__":
    main()

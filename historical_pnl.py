from __future__ import annotations

import time
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
from typing import Final

import numpy as np
import structlog

from core.types import DailyPnL, HistoricalPnLStore, LOOKBACK_DAYS, RiskUSD, Symbol

log = structlog.get_logger(__name__)

_SCENARIO_QUERY: Final[str] = """
    SELECT toFloat64(rf_return)
    FROM financial_dre.daily_pnl_history
    WHERE symbol = %(symbol)s
      AND date >= today() - {lookback_days}
    ORDER BY date ASC
    LIMIT {limit}
"""


class ClickHousePnLStore:
    def __init__(
        self,
        host    : str = "localhost",
        user    : str = "dre_user",
        password: str = "dre_pass",
        database: str = "financial_dre",
    ) -> None:
        import clickhouse_connect
        self._client = clickhouse_connect.get_client(
            host=host, username=user, password=password, database=database
        )

    def append(self, records: list[DailyPnL]) -> None:
        if not records:
            return
        rows = [[r.date, r.symbol, r.rf_return, r.scenario_pnl, r.actual_pnl] for r in records]
        self._client.insert(
            "daily_pnl_history", rows,
            column_names=["date", "symbol", "rf_return", "scenario_pnl", "actual_pnl"],
        )

    def get_window(self, symbol: Symbol, lookback_days: int = LOOKBACK_DAYS) -> np.ndarray:
        t0 = time.perf_counter()
        try:
            result = self._client.query(
                _SCENARIO_QUERY.format(lookback_days=lookback_days, limit=lookback_days + 5),
                parameters={"symbol": symbol},
            )
            if not result.result_rows:
                return np.array([], dtype=np.float64)
            arr = np.array([row[0] for row in result.result_rows], dtype=np.float64)
            elapsed_ms = (time.perf_counter() - t0) * 1000
            if elapsed_ms > 50:
                log.warning("pnl_fetch_slow", symbol=symbol, elapsed_ms=round(elapsed_ms, 1))
            return arr
        except Exception as e:
            log.error("pnl_fetch_failed", symbol=symbol, error=str(e))
            return np.array([], dtype=np.float64)


class InMemoryPnLStore:
    def __init__(self, max_days: int = LOOKBACK_DAYS) -> None:
        self._max_days = max_days
        self._data: dict[Symbol, deque[DailyPnL]] = defaultdict(lambda: deque(maxlen=max_days))

    def append(self, records: list[DailyPnL]) -> None:
        for r in records:
            self._data[r.symbol].append(r)

    def get_window(self, symbol: Symbol, lookback_days: int = LOOKBACK_DAYS) -> np.ndarray:
        records = list(self._data.get(symbol, []))
        if not records:
            return np.array([], dtype=np.float64)
        recent = records[-lookback_days:] if len(records) > lookback_days else records
        return np.array([r.rf_return for r in recent], dtype=np.float64)


class DailyPnLBuilder:
    def __init__(self, pnl_store: HistoricalPnLStore) -> None:
        self._store       = pnl_store
        self._prev_prices : dict[Symbol, float] = {}

    def record_eod_snapshot(
        self, symbol: Symbol, net_quantity: float, close_px: float, actual_pnl: RiskUSD,
    ) -> DailyPnL | None:
        prev_px = self._prev_prices.get(symbol)
        if prev_px is None or prev_px <= 0:
            self._prev_prices[symbol] = close_px
            return None
        rf_return    = float(np.log(close_px / prev_px))
        scenario_pnl = net_quantity * rf_return * close_px
        record = DailyPnL(
            date=datetime.now(timezone.utc), symbol=symbol,
            rf_return=rf_return, scenario_pnl=float(scenario_pnl), actual_pnl=float(actual_pnl),
        )
        self._prev_prices[symbol] = close_px
        self._store.append([record])
        return record

    def bulk_seed(
        self, symbol: Symbol, return_series: np.ndarray,
        current_notional: RiskUSD, start_date: datetime | None = None,
    ) -> None:
        if start_date is None:
            start_date = datetime.now(timezone.utc) - timedelta(days=len(return_series) + 50)
        records = []
        for i, rf_ret in enumerate(return_series):
            date = start_date + timedelta(days=i)
            if date.weekday() >= 5:
                continue
            records.append(DailyPnL(
                date=date, symbol=symbol, rf_return=float(rf_ret),
                scenario_pnl=float(rf_ret * current_notional),
                actual_pnl=float(rf_ret * current_notional),
            ))
        self._store.append(records)
        log.info("pnl_history_seeded", symbol=symbol, records=len(records))

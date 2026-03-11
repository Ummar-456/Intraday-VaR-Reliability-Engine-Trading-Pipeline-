from __future__ import annotations

import threading
import time
from datetime import datetime, timezone
from typing import Final

import numpy as np
import structlog

from core.types import MarketDataProvider, Price, Symbol, VolSurface

log = structlog.get_logger(__name__)

_RISK_FREE_RATE_USD: Final[float] = 0.0525

_EQUITY_SYMBOLS: Final[list[str]] = [
    "AAPL", "MSFT", "NVDA", "AMZN", "GOOGL",
    "TSLA", "JPM",  "GS",   "SPY",  "QQQ",
    "GLD",  "BTC/USD",
]

_CORR_MATRIX: Final[np.ndarray] = np.array([
    [1.00, 0.78, 0.65, 0.71, 0.74, 0.52, 0.55, 0.52, 0.82, 0.88, 0.05, 0.18],
    [0.78, 1.00, 0.71, 0.69, 0.79, 0.48, 0.58, 0.55, 0.81, 0.87, 0.03, 0.15],
    [0.65, 0.71, 1.00, 0.61, 0.68, 0.55, 0.48, 0.45, 0.74, 0.82, 0.02, 0.22],
    [0.71, 0.69, 0.61, 1.00, 0.72, 0.47, 0.52, 0.49, 0.79, 0.81, 0.04, 0.14],
    [0.74, 0.79, 0.68, 0.72, 1.00, 0.50, 0.54, 0.51, 0.80, 0.86, 0.03, 0.16],
    [0.52, 0.48, 0.55, 0.47, 0.50, 1.00, 0.41, 0.39, 0.65, 0.68, 0.01, 0.31],
    [0.55, 0.58, 0.48, 0.52, 0.54, 0.41, 1.00, 0.88, 0.78, 0.72, 0.12, 0.08],
    [0.52, 0.55, 0.45, 0.49, 0.51, 0.39, 0.88, 1.00, 0.75, 0.70, 0.11, 0.09],
    [0.82, 0.81, 0.74, 0.79, 0.80, 0.65, 0.78, 0.75, 1.00, 0.95, 0.08, 0.21],
    [0.88, 0.87, 0.82, 0.81, 0.86, 0.68, 0.72, 0.70, 0.95, 1.00, 0.06, 0.23],
    [0.05, 0.03, 0.02, 0.04, 0.03, 0.01, 0.12, 0.11, 0.08, 0.06, 1.00, 0.02],
    [0.18, 0.15, 0.22, 0.14, 0.16, 0.31, 0.08, 0.09, 0.21, 0.23, 0.02, 1.00],
], dtype=np.float64)

_ANNUAL_VOLS: Final[dict[str, float]] = {
    "AAPL": 0.2234, "MSFT": 0.1987, "NVDA": 0.4521, "AMZN": 0.2876,
    "GOOGL": 0.2345, "TSLA": 0.5678, "JPM": 0.2123, "GS": 0.2456,
    "SPY": 0.1423, "QQQ": 0.1867, "GLD": 0.1234, "BTC/USD": 0.7823,
    "ETH/USD": 0.8234, "EUR/USD": 0.0678, "GBP/USD": 0.0834,
    "USD/JPY": 0.0712, "IWM": 0.1956, "TLT": 0.1345,
    "MS": 0.2389, "BAC": 0.2534,
}

_INITIAL_SPOTS: Final[dict[str, float]] = {
    "AAPL": 189.50, "MSFT": 415.20, "NVDA": 875.00, "AMZN": 182.30,
    "GOOGL": 176.50, "TSLA": 242.80, "JPM": 198.40, "GS": 452.60,
    "SPY": 527.50, "QQQ": 451.20, "GLD": 218.40, "BTC/USD": 67_500.0,
    "ETH/USD": 3_450.0, "EUR/USD": 1.0842, "GBP/USD": 1.2634,
    "USD/JPY": 151.45, "IWM": 205.30, "TLT": 95.20,
    "MS": 101.20, "BAC": 38.50,
}


class SyntheticMarketData:
    def __init__(self, *, update_interval_s: float = 0.1, seed: int | None = None) -> None:
        self._rng      = np.random.default_rng(seed)
        self._interval = update_interval_s
        self._lock     = threading.RLock()
        self._spots    : dict[Symbol, float] = dict(_INITIAL_SPOTS)
        self._vol_cache: dict[Symbol, VolSurface] = {}
        self._rate     = _RISK_FREE_RATE_USD
        self._running  = False
        self._thread   : threading.Thread | None = None
        self._cholesky = self._build_cholesky()

    def get_spot(self, symbol: Symbol) -> Price | None:
        with self._lock:
            return self._spots.get(symbol)

    def get_vol_surface(self, symbol: Symbol) -> VolSurface | None:
        with self._lock:
            cached = self._vol_cache.get(symbol)
            if cached is None or (datetime.now(timezone.utc) - cached.fetched_at).total_seconds() > 60:
                cached = self._build_vol_surface(symbol)
                self._vol_cache[symbol] = cached
            return cached

    def get_risk_free_rate(self, currency: str = "USD") -> float:
        with self._lock:
            return self._rate

    def get_all_spots(self) -> dict[Symbol, float]:
        with self._lock:
            return dict(self._spots)

    def start(self) -> None:
        self._running = True
        self._thread  = threading.Thread(target=self._tick_loop, name="mktdata-tick", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._running = False
        if self._thread:
            self._thread.join(timeout=2.0)

    def __enter__(self) -> SyntheticMarketData:
        self.start()
        return self

    def __exit__(self, *_) -> None:
        self.stop()

    def generate_historical_returns(
        self, symbol: Symbol, n_days: int = 252, include_stress: bool = True,
    ) -> np.ndarray:
        vol_0 = _ANNUAL_VOLS.get(symbol, 0.25) / np.sqrt(252)
        omega, alpha, beta = vol_0 ** 2 * 0.1, 0.10, 0.85
        returns = np.zeros(n_days)
        h       = vol_0 ** 2
        for t in range(n_days):
            epsilon    = self._rng.standard_t(df=5) / np.sqrt(5 / 3)
            returns[t] = np.sqrt(h) * epsilon
            h          = omega + alpha * returns[t] ** 2 + beta * h
        if include_stress and n_days > 50:
            start  = self._rng.integers(20, n_days - 30)
            sign   = self._rng.choice([-1, 1])
            moves  = sign * np.array([2.0, 2.5, 3.0, 2.0, 1.5])
            returns[start:start + 5] = moves * vol_0 * np.sqrt(252)
        return returns.astype(np.float64)

    def _build_cholesky(self) -> np.ndarray:
        dt   = self._interval / (252 * 6.5 * 3600)
        vols = np.array([_ANNUAL_VOLS.get(s, 0.25) for s in _EQUITY_SYMBOLS])
        cov  = _CORR_MATRIX * np.outer(vols, vols) * dt
        cov += np.eye(len(_EQUITY_SYMBOLS)) * 1e-10
        try:
            return np.linalg.cholesky(cov)
        except np.linalg.LinAlgError:
            return np.diag(vols * np.sqrt(dt))

    def _tick_loop(self) -> None:
        while self._running:
            t0 = time.monotonic()
            self._update_spots()
            sleep = max(0, self._interval - (time.monotonic() - t0))
            time.sleep(sleep)

    def _update_spots(self) -> None:
        dt    = self._interval / (252 * 6.5 * 3600)
        z     = self._rng.standard_normal(len(_EQUITY_SYMBOLS))
        dW    = self._cholesky @ z
        vols  = np.array([_ANNUAL_VOLS.get(s, 0.25) for s in _EQUITY_SYMBOLS])
        drift = (self._rate - 0.5 * vols ** 2) * dt
        factor = np.exp(drift + dW)
        with self._lock:
            for i, sym in enumerate(_EQUITY_SYMBOLS):
                if sym in self._spots:
                    self._spots[sym] = max(self._spots[sym] * float(factor[i]), 0.001)
            for sym in ("EUR/USD", "GBP/USD", "USD/JPY"):
                if sym in self._spots:
                    vol_fx = _ANNUAL_VOLS.get(sym, 0.07)
                    z_fx   = self._rng.standard_normal()
                    self._spots[sym] *= float(np.exp(
                        (self._rate - 0.5 * vol_fx ** 2) * dt + vol_fx * np.sqrt(dt) * z_fx
                    ))
            kappa, theta, sigma_r = 0.5, _RISK_FREE_RATE_USD, 0.01
            self._rate += kappa * (theta - self._rate) * dt + sigma_r * np.sqrt(dt) * self._rng.standard_normal()
            self._rate  = float(np.clip(self._rate, 0.001, 0.20))

    def _build_vol_surface(self, symbol: Symbol) -> VolSurface:
        spot  = self._spots.get(symbol, 100.0)
        vol_0 = _ANNUAL_VOLS.get(symbol, 0.25)
        moneyness = np.linspace(0.70, 1.30, 15)
        strikes   = moneyness * spot
        expiries  = np.array([1/52, 2/52, 1/12, 2/12, 3/12, 6/12, 9/12, 1.0])
        a, b, rho, m, sigma_svi = vol_0 ** 2 * 0.8, vol_0 ** 2 * 0.3, -0.40, 0.0, 0.15
        vols = np.zeros((len(strikes), len(expiries)))
        for j, T in enumerate(expiries):
            term_factor = 1.0 + 0.10 * np.log(T * 12 + 1)
            for i, K in enumerate(strikes):
                k_log = np.log(K / spot)
                w     = a + b * (rho * (k_log - m) + np.sqrt((k_log - m) ** 2 + sigma_svi ** 2))
                w    *= term_factor
                vols[i, j] = np.sqrt(max(w / T, 0.001))
        return VolSurface(
            underlying=symbol, strikes=strikes, expiries=expiries,
            vols=vols, fetched_at=datetime.now(timezone.utc), source="synthetic_svi",
        )


class BloombergSAPIProvider:
    def __init__(self, host: str = "localhost", port: int = 8194) -> None:
        self._host = host
        self._port = port
        log.warning("bloomberg_sapi_stub", host=host, port=port)

    def get_spot(self, symbol: Symbol) -> Price | None:
        raise NotImplementedError("Connect real Bloomberg B-PIPE session")

    def get_vol_surface(self, symbol: Symbol) -> VolSurface | None:
        raise NotImplementedError("Connect real Bloomberg OVDV feed")

    def get_risk_free_rate(self, currency: str = "USD") -> float:
        raise NotImplementedError("Connect real Bloomberg rate feed")

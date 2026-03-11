from __future__ import annotations

import math
from datetime import datetime, timezone
from functools import lru_cache
from typing import Final

import numpy as np
import structlog

from core.types import (
    Greeks, GreeksCalculator, Instrument, MarketDataProvider,
    OptionType, Position, Price, RiskUSD, Symbol,
)

log = structlog.get_logger(__name__)

_EPSILON: Final[float] = 1e-10


def _norm_cdf(x: float) -> float:
    return 0.5 * math.erfc(-x / math.sqrt(2))


def _norm_pdf(x: float) -> float:
    return math.exp(-0.5 * x * x) / math.sqrt(2 * math.pi)


@lru_cache(maxsize=4096)
def _bs_greeks_cached(
    S: float, K: float, T: float, r: float,
    sigma: float, q: float, is_call: bool,
) -> tuple[float, float, float, float, float]:
    if T <= _EPSILON or sigma <= _EPSILON or S <= _EPSILON:
        return (0.0, 0.0, 0.0, 0.0, 0.0)

    sqrt_T = math.sqrt(T)
    d1     = (math.log(S / K) + (r - q + 0.5 * sigma ** 2) * T) / (sigma * sqrt_T)
    d2     = d1 - sigma * sqrt_T
    phi_d1 = _norm_pdf(d1)

    # Delta: call = e^(-qT)*N(d1)  |  put = -e^(-qT)*N(-d1)
    # put-call parity: delta_call - delta_put = e^(-qT)
    if is_call:
        delta = math.exp(-q * T) * _norm_cdf(d1)
    else:
        delta = -math.exp(-q * T) * _norm_cdf(-d1)

    # Gamma: identical for calls and puts
    gamma = math.exp(-q * T) * phi_d1 / (S * sigma * sqrt_T + _EPSILON)

    # Vega: per 1% vol move
    vega = S * math.exp(-q * T) * phi_d1 * sqrt_T * 0.01

    # Theta: per calendar day
    theta_base = (
        -S * math.exp(-q * T) * phi_d1 * sigma / (2 * sqrt_T)
        - r * K * math.exp(-r * T) * _norm_cdf(d2 if is_call else -d2)
        + q * S * math.exp(-q * T) * _norm_cdf(d1 if is_call else -d1)
    ) / 365
    if is_call:
        theta = theta_base
    else:
        theta = theta_base + r * K * math.exp(-r * T) / 365 - q * S * math.exp(-q * T) / 365

    # Rho: per 1% rate move
    if is_call:
        rho = K * T * math.exp(-r * T) * _norm_cdf(d2) * 0.01
    else:
        rho = -K * T * math.exp(-r * T) * _norm_cdf(-d2) * 0.01

    return (delta, gamma, vega, theta, rho)


class QuantLibGreeksCalculator:
    def __init__(self) -> None:
        self._quantlib_available = False
        try:
            import QuantLib as ql  # noqa: F401
            self._quantlib_available = True
        except ImportError:
            log.warning("quantlib_not_installed", fallback="analytical BS")

    def compute(self, position: Position, market_data: MarketDataProvider) -> Greeks:
        instr = position.instrument
        if instr is None:
            return self._zero(position.symbol)

        spot = market_data.get_spot(instr.symbol)
        if spot is None:
            return self._zero(position.symbol)

        if not instr.is_option:
            return self._linear(position, spot)

        if self._quantlib_available:
            return self._quantlib(position, instr, spot, market_data)
        return self._bs_analytical(position, instr, spot, market_data)

    def _linear(self, position: Position, spot: Price) -> Greeks:
        delta_usd = float(position.net_quantity) * spot
        return Greeks(
            symbol=position.symbol, delta_usd=delta_usd, gamma_usd=0.0,
            vega_usd=0.0, theta_usd=0.0, rho_usd=0.0,
            computed_at=datetime.now(timezone.utc),
        )

    def _bs_analytical(
        self, position: Position, instr: Instrument,
        spot: Price, market_data: MarketDataProvider,
    ) -> Greeks:
        if any(v is None for v in (instr.expiry, instr.strike, instr.option_type)):
            return self._zero(position.symbol)

        now = datetime.now(timezone.utc)
        T   = (instr.expiry - now).total_seconds() / (365.25 * 86400)
        if T <= 0:
            return self._zero(position.symbol)

        vol_surface = market_data.get_vol_surface(instr.underlying or instr.symbol)
        sigma = vol_surface.interpolate(instr.strike, T) if vol_surface else instr.volatility_30d
        r     = market_data.get_risk_free_rate()

        delta, gamma, vega, theta, rho = _bs_greeks_cached(
            round(spot, 4), round(instr.strike, 4), round(T, 6),
            round(r, 6), round(sigma, 6), 0.0,
            instr.option_type == OptionType.CALL,
        )
        qty  = float(position.net_quantity)
        mult = float(instr.lot_size)
        return Greeks(
            symbol      = position.symbol,
            delta_usd   = delta * qty * mult * spot,
            gamma_usd   = gamma * qty * mult * spot * spot * 0.01,
            vega_usd    = vega  * qty * mult,
            theta_usd   = theta * qty * mult,
            rho_usd     = rho   * qty * mult,
            computed_at = datetime.now(timezone.utc),
        )

    def _quantlib(
        self, position: Position, instr: Instrument,
        spot: Price, market_data: MarketDataProvider,
    ) -> Greeks:
        import QuantLib as ql
        if any(v is None for v in (instr.expiry, instr.strike, instr.option_type)):
            return self._zero(position.symbol)

        today = ql.Date.todaysDate()
        ql.Settings.instance().evaluationDate = today
        day_count   = ql.Actual365Fixed()
        expiry_date = ql.Date(instr.expiry.day, instr.expiry.month, instr.expiry.year)

        now     = datetime.now(timezone.utc)
        T_years = max(0.001, (instr.expiry - now).total_seconds() / (365.25 * 86400))
        vol_sf  = market_data.get_vol_surface(instr.underlying or instr.symbol)
        sigma   = vol_sf.interpolate(instr.strike, T_years) if vol_sf else instr.volatility_30d
        r       = market_data.get_risk_free_rate()

        bsm = ql.BlackScholesMertonProcess(
            ql.QuoteHandle(ql.SimpleQuote(spot)),
            ql.YieldTermStructureHandle(ql.FlatForward(today, 0.0, day_count)),
            ql.YieldTermStructureHandle(ql.FlatForward(today, r, day_count)),
            ql.BlackVolTermStructureHandle(
                ql.BlackConstantVol(today, ql.UnitedStates(ql.UnitedStates.NYSE), sigma, day_count)
            ),
        )
        opt_type = ql.Option.Call if instr.option_type == OptionType.CALL else ql.Option.Put
        option   = ql.VanillaOption(
            ql.PlainVanillaPayoff(opt_type, instr.strike),
            ql.EuropeanExercise(expiry_date),
        )
        option.setPricingEngine(ql.AnalyticEuropeanEngine(bsm))

        qty  = float(position.net_quantity)
        mult = float(instr.lot_size)
        return Greeks(
            symbol      = position.symbol,
            delta_usd   = option.delta() * qty * mult * spot,
            gamma_usd   = option.gamma() * qty * mult * spot ** 2 * 0.01,
            vega_usd    = option.vega()  * qty * mult * 0.01,
            theta_usd   = option.theta() * qty * mult / 365,
            rho_usd     = option.rho()   * qty * mult * 0.01,
            computed_at = datetime.now(timezone.utc),
        )

    @staticmethod
    def _zero(symbol: Symbol) -> Greeks:
        return Greeks(
            symbol=symbol, delta_usd=0.0, gamma_usd=0.0,
            vega_usd=0.0, theta_usd=0.0, rho_usd=0.0,
            computed_at=datetime.now(timezone.utc),
        )

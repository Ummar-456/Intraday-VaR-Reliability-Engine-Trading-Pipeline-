from __future__ import annotations

import math
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import numpy as np
import pytest

from core.types import (
    AssetClass, DailyPnL, ExecType, FIXExecution, Greeks, Instrument,
    OptionType, Position, SequenceNumber, Side, VaRMethod, VaRResult,
    LOOKBACK_DAYS,
)
from core.position_manager import PositionManager, SequenceGapError
from core.historical_pnl import InMemoryPnLStore, DailyPnLBuilder
from core.var_engine import VaREngine
from core.greeks import _bs_greeks_cached, QuantLibGreeksCalculator
from core.market_data import SyntheticMarketData


# ── Shared fixtures ────────────────────────────────────────────────────────────

class _MemStore:
    def __init__(self): self._p: dict = {}
    def get_position(self, symbol, book_id): return self._p.get(f"{symbol}:{book_id}")
    def upsert_position(self, p): self._p[f"{p.symbol}:{p.book_id}"] = p
    def get_all_positions(self, book_id): return list(self._p.values())
    def get_books(self): return []


def _ex(symbol, side, etype, qty, px, oid="O1", seq=1) -> FIXExecution:
    return FIXExecution(
        exec_id="E1", order_id=oid, cl_ord_id="C1", symbol=symbol,
        side=side, exec_type=etype, last_px=px,
        last_qty=Decimal(qty), cum_qty=Decimal(qty), avg_px=px,
        transact_time=datetime.now(timezone.utc),
        venue="NYSE", sequence_no=SequenceNumber(seq),
    )


def _instrument(symbol: str, vol: float = 0.25) -> Instrument:
    return Instrument(
        symbol=symbol, instrument_id=symbol, asset_class=AssetClass.EQUITY,
        currency="USD", exchange="TEST", tick_size=0.01, lot_size=Decimal("1"),
        volatility_30d=vol, volatility_252d=vol, beta=1.0,
    )


def _pos(symbol: str, qty: float, px: float, vol: float = 0.25) -> Position:
    return Position(
        symbol=symbol, book_id="t", net_quantity=Decimal(str(qty)),
        average_cost=px, realized_pnl=Decimal("0"), unrealized_pnl=Decimal("0"),
        last_px=px, last_updated=datetime.now(timezone.utc),
        instrument=_instrument(symbol, vol),
    )


def _pm() -> PositionManager:
    return PositionManager(_MemStore(), "d", gap_halt_threshold=10)


def _seed(store: InMemoryPnLStore, symbol: str, n: int = 250, vol: float = 0.25) -> None:
    rng     = np.random.default_rng(42)
    returns = rng.normal(0, vol / math.sqrt(252), n)
    base    = datetime.now(timezone.utc) - timedelta(days=n)
    store.append([
        DailyPnL(base + timedelta(days=i), symbol, float(r), float(r * 1e5), float(r * 1e5))
        for i, r in enumerate(returns)
    ])


# ── PositionManager ────────────────────────────────────────────────────────────

class TestPositionManager:

    def test_single_buy_creates_long(self):
        pm  = _pm()
        pos = pm.apply_execution(_ex("AAPL", Side.BUY, ExecType.FILL, "100", 189.50))
        assert pos.net_quantity == Decimal("100")
        assert abs(pos.average_cost - 189.50) < 0.001
        assert pos.realized_pnl == Decimal("0")

    def test_vwap_two_fills(self):
        pm = _pm()
        pm.apply_execution(_ex("AAPL", Side.BUY, ExecType.PARTIAL_FILL, "100", 189.50, seq=1))
        p  = pm.apply_execution(_ex("AAPL", Side.BUY, ExecType.FILL, "200", 191.00, seq=2))
        expected = (100 * 189.50 + 200 * 191.00) / 300
        assert abs(p.average_cost - expected) < 0.01
        assert p.net_quantity == Decimal("300")

    def test_partial_sell_realizes_pnl(self):
        pm = _pm()
        pm.apply_execution(_ex("AAPL", Side.BUY,  ExecType.FILL, "100", 189.50, "O1", 1))
        p  = pm.apply_execution(_ex("AAPL", Side.SELL, ExecType.FILL, "50",  195.00, "O2", 2))
        assert p.realized_pnl == Decimal("275.00")
        assert abs(p.average_cost - 189.50) < 0.001
        assert p.net_quantity == Decimal("50")

    def test_full_close(self):
        pm = _pm()
        pm.apply_execution(_ex("AAPL", Side.BUY,  ExecType.FILL, "100", 189.50, "O1", 1))
        p  = pm.apply_execution(_ex("AAPL", Side.SELL, ExecType.FILL, "100", 195.00, "O2", 2))
        assert p.net_quantity == Decimal("0")
        assert p.is_flat
        assert p.realized_pnl == Decimal("550.00")

    def test_flip_long_to_short(self):
        pm = _pm()
        pm.apply_execution(_ex("AAPL", Side.BUY,  ExecType.FILL, "100", 189.50, "O1", 1))
        p  = pm.apply_execution(_ex("AAPL", Side.SELL, ExecType.FILL, "150", 195.00, "O2", 2))
        assert p.net_quantity == Decimal("-50")
        assert p.realized_pnl == Decimal("550.00")
        assert abs(p.average_cost - 195.00) < 0.001

    def test_new_order_returns_none(self):
        assert _pm().apply_execution(_ex("AAPL", Side.BUY, ExecType.NEW, "100", 189.50)) is None

    def test_cancel_returns_none(self):
        assert _pm().apply_execution(_ex("AAPL", Side.BUY, ExecType.CANCELED, "100", 189.50)) is None

    def test_gap_below_threshold_continues(self):
        pm = _pm()
        pm.apply_execution(_ex("AAPL", Side.BUY, ExecType.FILL, "100", 189.50, seq=1))
        p  = pm.apply_execution(_ex("AAPL", Side.BUY, ExecType.FILL, "50",  190.00, seq=5))
        assert p is not None
        assert pm.stats["total_gaps"] == 1

    def test_large_gap_raises(self):
        pm = _pm()
        pm.apply_execution(_ex("AAPL", Side.BUY, ExecType.FILL, "100", 189.50, seq=1))
        with pytest.raises(SequenceGapError):
            pm.apply_execution(_ex("AAPL", Side.BUY, ExecType.FILL, "50", 190.00, seq=20))

    def test_mark_to_market(self):
        pm = _pm()
        pm.apply_execution(_ex("AAPL", Side.BUY, ExecType.FILL, "100", 189.50))
        p  = pm.mark_to_market("AAPL", 200.00)
        assert p.unrealized_pnl == Decimal("100") * (Decimal("200.00") - Decimal("189.50"))

    def test_corporate_action_split(self):
        from core.position_manager import CorporateAction
        pm = _pm()
        pm.apply_execution(_ex("AAPL", Side.BUY, ExecType.FILL, "100", 189.50))
        pm.apply_corporate_action(CorporateAction(
            symbol="AAPL", action_type="split",
            effective_date=datetime.now(timezone.utc), ratio=Decimal("2"),
        ))
        p = pm.get_position("AAPL")
        assert p.net_quantity == Decimal("200")
        assert abs(p.average_cost - 94.75) < 0.001


# ── Black-Scholes Greeks ───────────────────────────────────────────────────────

class TestBlackScholes:
    _ATM = dict(S=100.0, K=100.0, T=1.0, r=0.05, sigma=0.20, q=0.0, is_call=True)

    def test_atm_call_delta_range(self):
        d, *_ = _bs_greeks_cached(**self._ATM)
        assert 0.50 < d < 0.65

    def test_put_call_parity(self):
        dc, *_ = _bs_greeks_cached(**self._ATM)
        dp, *_ = _bs_greeks_cached(**{**self._ATM, "is_call": False})
        assert abs((dc - dp) - 1.0) < 0.001

    def test_put_delta_negative(self):
        dp, *_ = _bs_greeks_cached(**{**self._ATM, "is_call": False})
        assert -1 < dp < 0

    def test_gamma_same_call_put(self):
        _, gc, *_ = _bs_greeks_cached(**self._ATM)
        _, gp, *_ = _bs_greeks_cached(**{**self._ATM, "is_call": False})
        assert abs(gc - gp) < 1e-10

    def test_vega_same_call_put(self):
        _, _, vc, *_ = _bs_greeks_cached(**self._ATM)
        _, _, vp, *_ = _bs_greeks_cached(**{**self._ATM, "is_call": False})
        assert abs(vc - vp) < 1e-10

    def test_theta_negative_long_call(self):
        _, _, _, th, _ = _bs_greeks_cached(**self._ATM)
        assert th < 0

    def test_deep_itm_delta(self):
        d, *_ = _bs_greeks_cached(S=150.0, K=100.0, T=0.5, r=0.05, sigma=0.20, q=0.0, is_call=True)
        assert d > 0.95

    def test_deep_otm_delta(self):
        d, *_ = _bs_greeks_cached(S=50.0, K=100.0, T=0.5, r=0.05, sigma=0.20, q=0.0, is_call=True)
        assert d < 0.05

    def test_expired_all_zero(self):
        result = _bs_greeks_cached(S=100.0, K=100.0, T=0.0, r=0.05, sigma=0.20, q=0.0, is_call=True)
        assert all(v == 0.0 for v in result)

    def test_cache_hit(self):
        _bs_greeks_cached.cache_clear()
        _bs_greeks_cached(**self._ATM)
        _bs_greeks_cached(**self._ATM)
        info = _bs_greeks_cached.cache_info()
        assert info.hits == 1 and info.misses == 1

    def test_linear_greeks_delta_equals_notional(self):
        calc = QuantLibGreeksCalculator()
        md   = SyntheticMarketData(seed=42)
        spot = md.get_spot("AAPL") or 189.50
        pos  = _pos("AAPL", 100, spot)
        g    = calc.compute(pos, md)
        assert abs(g.delta_usd - (100 * spot)) < 1.0
        assert g.gamma_usd == 0.0
        assert g.vega_usd  == 0.0


# ── VaR Engine ─────────────────────────────────────────────────────────────────

class TestVaREngine:

    def _engine(self, store, **kw) -> VaREngine:
        return VaREngine(store, **{"compute_stressed": False, "run_kupiec": False, **kw})

    def test_var_non_negative(self):
        s = InMemoryPnLStore(); _seed(s, "AAPL")
        r = self._engine(s, confidence=0.99).compute([_pos("AAPL", 100, 189.5)], {}, "t1")
        assert r.portfolio_var >= 0

    def test_method_historical_with_sufficient_data(self):
        s = InMemoryPnLStore(); _seed(s, "AAPL")
        r = self._engine(s).compute([_pos("AAPL", 100, 189.5)], {}, "t2")
        assert r.method == VaRMethod.HISTORICAL

    def test_99_var_gte_975_var(self):
        s = InMemoryPnLStore(); _seed(s, "AAPL")
        p = _pos("AAPL", 1000, 189.5)
        v99  = self._engine(s, confidence=0.99 ).compute([p], {}, "t99" ).portfolio_var
        v975 = self._engine(s, confidence=0.975).compute([p], {}, "t975").portfolio_var
        assert v99 >= v975

    def test_larger_position_larger_var(self):
        s = InMemoryPnLStore(); _seed(s, "AAPL")
        vs = self._engine(s).compute([_pos("AAPL",  100, 189.5)], {}, "ts").portfolio_var
        vl = self._engine(s).compute([_pos("AAPL", 1000, 189.5)], {}, "tl").portfolio_var
        assert vl > vs

    def test_diversification_benefit_non_negative(self):
        s = InMemoryPnLStore(); _seed(s, "AAPL"); _seed(s, "GLD", vol=0.12)
        r = self._engine(s).compute([_pos("AAPL", 1000, 189.5, 0.22), _pos("GLD", 1000, 218.4, 0.12)], {}, "td")
        assert r.diversification_ben >= 0

    def test_parametric_fallback_insufficient_history(self):
        s = InMemoryPnLStore(); _seed(s, "AAPL", n=10)
        r = self._engine(s).compute([_pos("AAPL", 100, 189.5)], {}, "tfb")
        assert r.portfolio_var > 0

    def test_flat_position_excluded(self):
        r = self._engine(InMemoryPnLStore()).compute([_pos("AAPL", 0, 189.5)], {}, "tf")
        assert r.portfolio_var == 0.0

    def test_stressed_var_gte_normal_var(self):
        s = InMemoryPnLStore(); _seed(s, "AAPL")
        r = VaREngine(s, confidence=0.99, compute_stressed=True, run_kupiec=False)\
            .compute([_pos("AAPL", 1000, 189.5)], {}, "tst")
        assert r.stressed_var >= r.portfolio_var

    def test_slo_not_breached_small_book(self):
        import time
        s = InMemoryPnLStore()
        positions = []
        for sym, px, vol in [("AAPL", 189.5, 0.22), ("MSFT", 415.2, 0.20), ("SPY", 527.5, 0.14)]:
            _seed(s, sym, vol=vol)
            positions.append(_pos(sym, 100, px, vol))
        t0     = time.perf_counter()
        result = self._engine(s).compute(positions, {}, "tslo")
        assert (time.perf_counter() - t0) * 1000 < 1000


# ── Kupiec POF ─────────────────────────────────────────────────────────────────

class TestKupiec:

    def test_zero_exceptions(self):
        assert VaREngine._kupiec_pof_test(0, 60) == 1.0

    def test_one_exception_passes(self):
        assert VaREngine._kupiec_pof_test(1, 60) > 0.05

    def test_ten_exceptions_rejects(self):
        assert VaREngine._kupiec_pof_test(10, 60) < 0.05


# ── Traffic Light ──────────────────────────────────────────────────────────────

def _result(exc: int) -> VaRResult:
    return VaRResult(
        calculated_at=datetime.now(timezone.utc), method=VaRMethod.HISTORICAL,
        confidence=0.99, horizon_days=1, portfolio_var=1e6, stressed_var=1.5e6,
        component_var={}, diversification_ben=0.0, lookback_days=250, scenario_count=250,
        worst_scenario_date=None, calculation_ms=5000.0, dag_run_id="t", exceptions_60d=exc,
    )

class TestTrafficLight:
    def test_green(self): assert _result(3).traffic_light == "green"
    def test_amber(self): assert _result(7).traffic_light == "amber"
    def test_red(self):   assert _result(12).traffic_light == "red"


# ── Vol Surface ────────────────────────────────────────────────────────────────

class TestVolSurface:

    def test_all_vols_positive(self):
        md   = SyntheticMarketData(seed=42)
        surf = md._build_vol_surface("AAPL")
        assert np.all(surf.vols > 0)

    def test_put_skew_equity(self):
        md   = SyntheticMarketData(seed=42)
        surf = md._build_vol_surface("AAPL")
        spot = md._spots["AAPL"]
        otm_put  = surf.interpolate(spot * 0.70, 0.25)
        otm_call = surf.interpolate(spot * 1.30, 0.25)
        assert otm_put > otm_call

    def test_interpolated_vol_reasonable(self):
        md   = SyntheticMarketData(seed=42)
        surf = md._build_vol_surface("AAPL")
        vol  = surf.interpolate(md._spots["AAPL"], 0.25)
        assert 0.05 < vol < 2.0

    def test_term_structure_exists(self):
        md   = SyntheticMarketData(seed=42)
        surf = md._build_vol_surface("SPY")
        spot = md._spots["SPY"]
        v1m  = surf.interpolate(spot, 1 / 12)
        v1y  = surf.interpolate(spot, 1.0)
        assert v1m > 0 and v1y > 0


# ── Historical PnL Store ───────────────────────────────────────────────────────

class TestHistoricalPnLStore:

    def test_append_and_retrieve(self):
        store = InMemoryPnLStore()
        _seed(store, "AAPL", n=50)
        window = store.get_window("AAPL", 50)
        assert len(window) == 50

    def test_order_preserved(self):
        store   = InMemoryPnLStore()
        returns = [0.01, 0.02, -0.01, 0.03, -0.02]
        store.append([
            DailyPnL(datetime.now(timezone.utc), "AAPL", r, r * 1000, r * 1000)
            for r in returns
        ])
        np.testing.assert_array_almost_equal(store.get_window("AAPL"), returns)

    def test_empty_returns_empty_array(self):
        window = InMemoryPnLStore().get_window("NONE")
        assert isinstance(window, np.ndarray) and len(window) == 0

    def test_lookback_truncates(self):
        store = InMemoryPnLStore(max_days=300)
        _seed(store, "AAPL", n=300)
        assert len(store.get_window("AAPL", 100)) == 100

    def test_bulk_seed_skips_weekends(self):
        from core.historical_pnl import DailyPnLBuilder
        store   = InMemoryPnLStore()
        builder = DailyPnLBuilder(store)
        returns = np.random.default_rng(0).normal(0, 0.01, 20)
        builder.bulk_seed("AAPL", returns, 100_000.0)
        window = store.get_window("AAPL")
        assert 0 < len(window) <= 20

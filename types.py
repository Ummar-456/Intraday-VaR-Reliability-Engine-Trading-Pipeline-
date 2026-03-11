from __future__ import annotations

import enum
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Final, Literal, NewType, Protocol, TypeAlias, runtime_checkable

import numpy as np

Symbol: TypeAlias       = str
InstrumentId: TypeAlias = str
DagRunId: TypeAlias     = str
Notional: TypeAlias     = Decimal
Price: TypeAlias        = float
Quantity: TypeAlias     = Decimal
RiskUSD: TypeAlias      = float
SequenceNumber          = NewType("SequenceNumber", int)


class Side(enum.StrEnum):
    BUY  = "BUY"
    SELL = "SELL"

    def sign(self) -> int:
        return 1 if self is Side.BUY else -1


class AssetClass(enum.StrEnum):
    EQUITY = "equity"
    FX     = "fx"
    CRYPTO = "crypto"
    ETF    = "etf"
    OPTION = "option"
    FUTURE = "future"
    BOND   = "bond"


class ExecType(enum.StrEnum):
    NEW          = "NEW"
    PARTIAL_FILL = "PARTIAL_FILL"
    FILL         = "FILL"
    CANCELED     = "CANCELED"
    REPLACED     = "REPLACED"
    REJECTED     = "REJECTED"
    TRADE        = "TRADE"


class OptionType(enum.StrEnum):
    CALL = "CALL"
    PUT  = "PUT"


class VaRMethod(enum.StrEnum):
    HISTORICAL = "historical"
    PARAMETRIC = "parametric"
    STRESSED   = "stressed"


@dataclass(slots=True, frozen=True)
class Instrument:
    symbol          : Symbol
    instrument_id   : InstrumentId
    asset_class     : AssetClass
    currency        : str
    exchange        : str
    tick_size       : Price
    lot_size        : Quantity
    volatility_30d  : float
    volatility_252d : float
    beta            : float
    option_type     : OptionType | None = None
    strike          : Price | None      = None
    expiry          : datetime | None   = None
    underlying      : Symbol | None     = None
    var_limit_1d    : RiskUSD           = 0.0
    position_limit  : Quantity          = field(default_factory=lambda: Decimal(0))

    @property
    def is_option(self) -> bool:
        return self.option_type is not None

    @property
    def daily_vol(self) -> float:
        return self.volatility_30d / np.sqrt(252)


@dataclass(slots=True)
class FIXExecution:
    exec_id      : str
    order_id     : str
    cl_ord_id    : str
    symbol       : Symbol
    side         : Side
    exec_type    : ExecType
    last_px      : Price
    last_qty     : Quantity
    cum_qty      : Quantity
    avg_px       : Price
    transact_time: datetime
    venue        : str
    sequence_no  : SequenceNumber
    instrument   : Instrument | None = None


@dataclass(slots=True)
class Position:
    symbol        : Symbol
    book_id       : str
    net_quantity  : Quantity
    average_cost  : Price
    realized_pnl  : Decimal
    unrealized_pnl: Decimal
    last_px       : Price
    last_updated  : datetime
    instrument    : Instrument | None = None
    fill_count    : int               = 0
    total_notional: Decimal           = field(default_factory=lambda: Decimal(0))

    @property
    def is_flat(self) -> bool:
        return self.net_quantity == 0

    @property
    def side(self) -> Side | None:
        if self.net_quantity > 0: return Side.BUY
        if self.net_quantity < 0: return Side.SELL
        return None

    def market_value(self) -> Decimal:
        return Decimal(str(self.last_px)) * self.net_quantity


@dataclass(slots=True, frozen=True)
class Greeks:
    symbol     : Symbol
    delta_usd  : RiskUSD
    gamma_usd  : RiskUSD
    vega_usd   : RiskUSD
    theta_usd  : RiskUSD
    rho_usd    : RiskUSD
    computed_at: datetime

    def total_risk_usd(self) -> RiskUSD:
        return abs(self.delta_usd) + abs(self.vega_usd) * 0.1


@dataclass(slots=True)
class VolSurface:
    underlying: Symbol
    strikes   : np.ndarray
    expiries  : np.ndarray
    vols      : np.ndarray
    fetched_at: datetime
    source    : str

    def __post_init__(self) -> None:
        assert self.vols.shape == (len(self.strikes), len(self.expiries))

    def interpolate(self, strike: float, expiry_years: float) -> float:
        i  = np.searchsorted(self.strikes, strike).clip(1, len(self.strikes) - 1)
        j  = np.searchsorted(self.expiries, expiry_years).clip(1, len(self.expiries) - 1)
        s0, s1 = self.strikes[i - 1], self.strikes[i]
        e0, e1 = self.expiries[j - 1], self.expiries[j]
        ws = (strike - s0) / (s1 - s0 + 1e-10)
        we = (expiry_years - e0) / (e1 - e0 + 1e-10)
        v  = (
            self.vols[i - 1, j - 1] * (1 - ws) * (1 - we)
            + self.vols[i,     j - 1] * ws       * (1 - we)
            + self.vols[i - 1, j    ] * (1 - ws) * we
            + self.vols[i,     j    ] * ws        * we
        )
        return float(np.clip(v, 0.001, 5.0))


@dataclass(slots=True, frozen=True)
class DailyPnL:
    date        : datetime
    symbol      : Symbol
    rf_return   : float
    scenario_pnl: RiskUSD
    actual_pnl  : RiskUSD


@dataclass(slots=True)
class VaRResult:
    calculated_at       : datetime
    method              : VaRMethod
    confidence          : float
    horizon_days        : int
    portfolio_var       : RiskUSD
    stressed_var        : RiskUSD
    component_var       : dict[Symbol, RiskUSD]
    diversification_ben : RiskUSD
    lookback_days       : int
    scenario_count      : int
    worst_scenario_date : datetime | None
    calculation_ms      : float
    dag_run_id          : DagRunId
    exceptions_60d      : int   = 0
    kupiec_pvalue       : float = 1.0

    @property
    def is_slo_breached(self) -> bool:
        return self.calculation_ms > 45_000

    @property
    def traffic_light(self) -> Literal["green", "amber", "red"]:
        if self.exceptions_60d <= 4: return "green"
        if self.exceptions_60d <= 9: return "amber"
        return "red"


@runtime_checkable
class PositionStore(Protocol):
    def get_position(self, symbol: Symbol, book_id: str) -> Position | None: ...
    def upsert_position(self, position: Position) -> None: ...
    def get_all_positions(self, book_id: str) -> list[Position]: ...
    def get_books(self) -> list[str]: ...


@runtime_checkable
class MarketDataProvider(Protocol):
    def get_spot(self, symbol: Symbol) -> Price | None: ...
    def get_vol_surface(self, symbol: Symbol) -> VolSurface | None: ...
    def get_risk_free_rate(self, currency: str = "USD") -> float: ...


@runtime_checkable
class GreeksCalculator(Protocol):
    def compute(self, position: Position, market_data: MarketDataProvider) -> Greeks: ...


@runtime_checkable
class HistoricalPnLStore(Protocol):
    def append(self, records: list[DailyPnL]) -> None: ...
    def get_window(self, symbol: Symbol, lookback_days: int = 250) -> np.ndarray: ...


@runtime_checkable
class VaRAuditStore(Protocol):
    def write_result(self, result: VaRResult) -> None: ...
    def get_exceptions(self, lookback_days: int = 60) -> int: ...


Z_99: Final[float]           = 2.3263
Z_975: Final[float]          = 1.9600
SQRT_252: Final[float]       = np.sqrt(252)
SLO_SECONDS: Final[int]      = 45
LOOKBACK_DAYS: Final[int]    = 250
MIN_HISTORY_DAYS: Final[int] = 30

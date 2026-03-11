from __future__ import annotations

import copy
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_EVEN
from typing import Final

import structlog

from core.types import (
    ExecType, FIXExecution, Instrument, Position,
    PositionStore, Price, Quantity, SequenceNumber, Side, Symbol,
)

log = structlog.get_logger(__name__)

_ZERO: Final[Decimal]    = Decimal("0")
_TWO_DP: Final[str]      = "0.01"
_EIGHT_DP: Final[str]    = "0.00000001"


class OrderState:
    __slots__ = ("order_id", "symbol", "side", "ordered_qty", "cum_qty", "avg_px", "is_done")

    def __init__(self, order_id: str, symbol: Symbol, side: Side, ordered_qty: Quantity):
        self.order_id    = order_id
        self.symbol      = symbol
        self.side        = side
        self.ordered_qty = ordered_qty
        self.cum_qty     : Quantity = _ZERO
        self.avg_px      : Price    = 0.0
        self.is_done     : bool     = False

    def apply_fill(self, qty: Quantity, px: Price) -> None:
        prev_notional = self.cum_qty * Decimal(str(self.avg_px))
        fill_notional = qty * Decimal(str(px))
        self.cum_qty  += qty
        if self.cum_qty > _ZERO:
            self.avg_px = float((prev_notional + fill_notional) / self.cum_qty)
        if self.cum_qty >= self.ordered_qty:
            self.is_done = True


@dataclass(slots=True)
class SequenceGapDetector:
    _last_seq    : SequenceNumber = field(default_factory=lambda: SequenceNumber(0))
    _gap_count   : int            = 0
    _max_gap_seen: int            = 0

    def check(self, seq: SequenceNumber) -> int | None:
        if self._last_seq == 0:
            self._last_seq = seq
            return None
        gap = int(seq) - int(self._last_seq) - 1
        self._last_seq = seq
        if gap > 0:
            self._gap_count    += 1
            self._max_gap_seen  = max(self._max_gap_seen, gap)
            return gap
        if gap < 0:
            log.warning("out_of_order_sequence", received=int(seq))
        return None

    @property
    def total_gaps(self) -> int:
        return self._gap_count


@dataclass(slots=True, frozen=True)
class CorporateAction:
    symbol        : Symbol
    action_type   : str
    effective_date: datetime
    ratio         : Decimal
    new_symbol    : Symbol | None = None

    def adjust_position(self, pos: Position) -> Position:
        match self.action_type:
            case "split":
                new_qty  = pos.net_quantity * self.ratio
                new_cost = pos.average_cost / float(self.ratio)
            case "reverse_split":
                new_qty  = pos.net_quantity / self.ratio
                new_cost = pos.average_cost * float(self.ratio)
            case "dividend":
                new_qty  = pos.net_quantity
                new_cost = pos.average_cost - float(self.ratio)
            case _:
                log.warning("unhandled_corporate_action", action=self.action_type)
                return pos
        updated = copy.copy(pos)
        object.__setattr__(updated, "net_quantity", new_qty)
        object.__setattr__(updated, "average_cost", max(0.0, new_cost))
        return updated


class PositionManager:
    def __init__(
        self,
        store              : PositionStore,
        book_id            : str,
        *,
        gap_halt_threshold : int = 5,
    ) -> None:
        self._store              = store
        self._book_id            = book_id
        self._gap_halt_threshold = gap_halt_threshold
        self._orders             : dict[str, OrderState] = {}
        self._gap_detector       = SequenceGapDetector()
        self._lock               = threading.Lock()
        self._executions_count   = 0
        self._rejected_count     = 0
        self._last_processed_at  : datetime | None = None

    def apply_execution(self, exec_: FIXExecution) -> Position | None:
        with self._lock:
            return self._apply_execution_locked(exec_)

    def apply_corporate_action(self, action: CorporateAction) -> None:
        with self._lock:
            pos = self._store.get_position(action.symbol, self._book_id)
            if pos is None or pos.is_flat:
                return
            self._store.upsert_position(action.adjust_position(pos))

    def get_position(self, symbol: Symbol) -> Position | None:
        return self._store.get_position(symbol, self._book_id)

    def get_all_positions(self) -> list[Position]:
        return self._store.get_all_positions(self._book_id)

    def get_non_flat_positions(self) -> list[Position]:
        return [p for p in self.get_all_positions() if not p.is_flat]

    def mark_to_market(self, symbol: Symbol, market_px: Price) -> Position | None:
        with self._lock:
            pos = self._store.get_position(symbol, self._book_id)
            if pos is None or pos.is_flat:
                return pos
            px_dec    = Decimal(str(market_px))
            cost_dec  = Decimal(str(pos.average_cost))
            direction = Decimal("1") if pos.net_quantity > _ZERO else Decimal("-1")
            unrealized = (px_dec - cost_dec) * abs(pos.net_quantity) * direction
            updated = copy.copy(pos)
            object.__setattr__(updated, "unrealized_pnl", unrealized.quantize(Decimal(_TWO_DP), ROUND_HALF_EVEN))
            object.__setattr__(updated, "last_px",        market_px)
            object.__setattr__(updated, "last_updated",   datetime.now(timezone.utc))
            self._store.upsert_position(updated)
            return updated

    @property
    def stats(self) -> dict:
        return {
            "book_id"       : self._book_id,
            "executions"    : self._executions_count,
            "rejected"      : self._rejected_count,
            "total_gaps"    : self._gap_detector.total_gaps,
            "open_orders"   : sum(1 for o in self._orders.values() if not o.is_done),
            "last_processed": self._last_processed_at.isoformat() if self._last_processed_at else None,
        }

    def _apply_execution_locked(self, exec_: FIXExecution) -> Position | None:
        gap = self._gap_detector.check(exec_.sequence_no)
        if gap is not None:
            log.error("sequence_gap_detected", gap=gap, seq=int(exec_.sequence_no))
            if gap >= self._gap_halt_threshold:
                raise SequenceGapError(
                    f"Gap of {gap} exceeds halt threshold {self._gap_halt_threshold}."
                )
        match exec_.exec_type:
            case ExecType.NEW:
                return self._handle_new(exec_)
            case ExecType.PARTIAL_FILL | ExecType.FILL | ExecType.TRADE:
                return self._handle_fill(exec_)
            case ExecType.CANCELED | ExecType.REJECTED:
                return self._handle_cancel(exec_)
            case ExecType.REPLACED:
                return self._handle_replace(exec_)
            case _:
                return None

    def _handle_new(self, exec_: FIXExecution) -> None:
        if exec_.order_id not in self._orders:
            self._orders[exec_.order_id] = OrderState(
                exec_.order_id, exec_.symbol, exec_.side, exec_.last_qty
            )
        return None

    def _handle_fill(self, exec_: FIXExecution) -> Position:
        order = self._orders.get(exec_.order_id)
        if order is None:
            order = OrderState(exec_.order_id, exec_.symbol, exec_.side, exec_.cum_qty)
            self._orders[exec_.order_id] = order
        order.apply_fill(exec_.last_qty, exec_.last_px)
        self._executions_count += 1
        self._last_processed_at = exec_.transact_time

        signed_delta = Decimal(str(exec_.last_qty)) * Decimal(str(exec_.side.sign()))
        pos = self._store.get_position(exec_.symbol, self._book_id)
        if pos is None:
            pos = Position(
                symbol=exec_.symbol, book_id=self._book_id,
                net_quantity=_ZERO, average_cost=0.0,
                realized_pnl=_ZERO, unrealized_pnl=_ZERO,
                last_px=exec_.last_px, last_updated=exec_.transact_time,
                instrument=exec_.instrument,
            )
        updated = self._update_position(pos, signed_delta, exec_.last_px)
        self._store.upsert_position(updated)
        return updated

    def _handle_cancel(self, exec_: FIXExecution) -> None:
        order = self._orders.pop(exec_.order_id, None)
        if order:
            order.is_done = True
            self._rejected_count += 1
        return None

    def _handle_replace(self, exec_: FIXExecution) -> None:
        order = self._orders.get(exec_.order_id)
        if order:
            order.ordered_qty = exec_.cum_qty
        return None

    def _update_position(
        self,
        pos         : Position,
        signed_delta: Decimal,
        fill_px     : Price,
    ) -> Position:
        old_qty  = pos.net_quantity
        new_qty  = old_qty + signed_delta
        fill_dec = Decimal(str(fill_px))

        same_direction = (old_qty == _ZERO) or ((old_qty > _ZERO) == (signed_delta > _ZERO))

        if same_direction:
            if old_qty == _ZERO:
                new_avg_cost = fill_px
            else:
                old_notional = abs(old_qty) * Decimal(str(pos.average_cost))
                new_notional = abs(signed_delta) * fill_dec
                new_avg_cost = float((old_notional + new_notional) / abs(new_qty))
            realized_pnl = pos.realized_pnl
        else:
            closed_qty   = min(abs(old_qty), abs(signed_delta))
            direction    = Decimal("1") if old_qty > _ZERO else Decimal("-1")
            realized_pnl = (
                pos.realized_pnl
                + (fill_dec - Decimal(str(pos.average_cost))) * closed_qty * direction
            )
            new_avg_cost = pos.average_cost if abs(signed_delta) <= abs(old_qty) else fill_px

        new_avg_dec  = Decimal(str(new_avg_cost))
        direction_new = Decimal("1") if new_qty > _ZERO else Decimal("-1")
        unrealized    = (fill_dec - new_avg_dec) * abs(new_qty) * direction_new

        updated = copy.copy(pos)
        object.__setattr__(updated, "net_quantity",   new_qty)
        object.__setattr__(updated, "average_cost",   new_avg_cost)
        object.__setattr__(updated, "realized_pnl",   realized_pnl.quantize(Decimal(_TWO_DP), ROUND_HALF_EVEN))
        object.__setattr__(updated, "unrealized_pnl", unrealized.quantize(Decimal(_TWO_DP), ROUND_HALF_EVEN))
        object.__setattr__(updated, "last_px",        fill_px)
        object.__setattr__(updated, "last_updated",   datetime.now(timezone.utc))
        object.__setattr__(updated, "fill_count",     pos.fill_count + 1)
        object.__setattr__(updated, "total_notional", pos.total_notional + abs(signed_delta) * fill_dec)
        return updated


class SequenceGapError(RuntimeError):
    pass


class PositionIntegrityError(RuntimeError):
    pass

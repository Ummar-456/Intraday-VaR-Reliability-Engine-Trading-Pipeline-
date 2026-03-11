from __future__ import annotations

import json
from datetime import datetime, timezone
from decimal import Decimal

import structlog

from core.types import AssetClass, Instrument, Position, PositionStore, Symbol

log = structlog.get_logger(__name__)


class RedisPositionStore:
    def __init__(self, redis_url: str = "redis://localhost:6379/0") -> None:
        import redis
        self._r = redis.from_url(redis_url, decode_responses=True)

    def _key(self, symbol: Symbol, book_id: str) -> str:
        return f"pos:{book_id}:{symbol}"

    def get_position(self, symbol: Symbol, book_id: str) -> Position | None:
        raw = self._r.get(self._key(symbol, book_id))
        if raw is None:
            return None
        return self._deserialize(json.loads(raw))

    def upsert_position(self, position: Position) -> None:
        key  = self._key(position.symbol, position.book_id)
        data = self._serialize(position)
        self._r.set(key, json.dumps(data))
        self._r.sadd(f"book:{position.book_id}:symbols", position.symbol)

    def get_all_positions(self, book_id: str) -> list[Position]:
        symbols = self._r.smembers(f"book:{book_id}:symbols")
        result  = []
        for sym in symbols:
            pos = self.get_position(sym, book_id)
            if pos is not None:
                result.append(pos)
        return result

    def get_books(self) -> list[str]:
        keys = self._r.keys("book:*:symbols")
        return [k.split(":")[1] for k in keys]

    @staticmethod
    def _serialize(pos: Position) -> dict:
        return {
            "symbol":         pos.symbol,
            "book_id":        pos.book_id,
            "net_quantity":   str(pos.net_quantity),
            "average_cost":   pos.average_cost,
            "realized_pnl":   str(pos.realized_pnl),
            "unrealized_pnl": str(pos.unrealized_pnl),
            "last_px":        pos.last_px,
            "last_updated":   pos.last_updated.isoformat(),
            "fill_count":     pos.fill_count,
            "total_notional": str(pos.total_notional),
            "asset_class":    pos.instrument.asset_class.value if pos.instrument else "equity",
            "volatility_30d": pos.instrument.volatility_30d  if pos.instrument else 0.25,
        }

    @staticmethod
    def _deserialize(d: dict) -> Position:
        instr = Instrument(
            symbol=d["symbol"], instrument_id=d["symbol"],
            asset_class=AssetClass(d.get("asset_class", "equity")),
            currency="USD", exchange="LIVE", tick_size=0.01,
            lot_size=Decimal("1"), volatility_30d=d.get("volatility_30d", 0.25),
            volatility_252d=d.get("volatility_30d", 0.25), beta=1.0,
        )
        return Position(
            symbol=d["symbol"], book_id=d["book_id"],
            net_quantity=Decimal(d["net_quantity"]),
            average_cost=float(d["average_cost"]),
            realized_pnl=Decimal(d["realized_pnl"]),
            unrealized_pnl=Decimal(d["unrealized_pnl"]),
            last_px=float(d["last_px"]),
            last_updated=datetime.fromisoformat(d["last_updated"]),
            instrument=instr,
            fill_count=int(d.get("fill_count", 0)),
            total_notional=Decimal(d.get("total_notional", "0")),
        )


class ClickHouseSnapshotPositionStore:
    """
    Read positions from ClickHouse snapshots.
    Used by the Airflow DAG task_fetch_positions.
    Writes go through RedisPositionStore (hot path) and are flushed to ClickHouse by the consumer.
    """
    def __init__(self, client, book_id: str) -> None:
        self._ch      = client
        self._book_id = book_id

    def get_position(self, symbol: Symbol, book_id: str) -> Position | None:
        result = self._ch.query(
            "SELECT net_quantity, average_cost, realized_pnl, unrealized_pnl, last_px "
            "FROM financial_dre.position_snapshots "
            "WHERE book_id = %(book_id)s AND symbol = %(symbol)s "
            "ORDER BY snapshot_at DESC LIMIT 1",
            parameters={"book_id": book_id, "symbol": symbol},
        )
        if not result.result_rows:
            return None
        row = result.result_rows[0]
        return Position(
            symbol=symbol, book_id=book_id,
            net_quantity=Decimal(str(row[0])), average_cost=float(row[1]),
            realized_pnl=Decimal(str(row[2])), unrealized_pnl=Decimal(str(row[3])),
            last_px=float(row[4]), last_updated=datetime.now(timezone.utc),
        )

    def upsert_position(self, position: Position) -> None:
        self._ch.insert("financial_dre.position_snapshots", [[
            datetime.now(timezone.utc), position.book_id, position.symbol,
            float(position.net_quantity), position.average_cost,
            float(position.realized_pnl), float(position.unrealized_pnl),
            position.last_px,
        ]], column_names=[
            "snapshot_at", "book_id", "symbol", "net_quantity",
            "average_cost", "realized_pnl", "unrealized_pnl", "last_px",
        ])

    def get_all_positions(self, book_id: str) -> list[Position]:
        result = self._ch.query(
            "SELECT symbol, net_quantity, average_cost, realized_pnl, unrealized_pnl, last_px "
            "FROM financial_dre.position_snapshots "
            "WHERE book_id = %(book_id)s AND net_quantity != 0",
            parameters={"book_id": book_id},
        )
        return [
            Position(
                symbol=row[0], book_id=book_id,
                net_quantity=Decimal(str(row[1])), average_cost=float(row[2]),
                realized_pnl=Decimal(str(row[3])), unrealized_pnl=Decimal(str(row[4])),
                last_px=float(row[5]), last_updated=datetime.now(timezone.utc),
            )
            for row in result.result_rows
        ]

    def get_books(self) -> list[str]:
        result = self._ch.query(
            "SELECT DISTINCT book_id FROM financial_dre.position_snapshots"
        )
        return [row[0] for row in result.result_rows]

from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Dict, List

MARKET_DIRECTORY_SCHEMA = "2finance.market_directory.v1"
MARKET_DEFINITION_SCHEMA = "2finance.market_definition.v1"


@dataclass(frozen=True)
class MarketDefinition:
    """Typed binding of the language-neutral 2Finance Market Directory contract."""

    market_id: str
    symbol: str
    engine_id: str
    symbol_id: int
    route_epoch: int
    status: str
    websocket_endpoint: str
    tick_size: Decimal
    step_size: Decimal
    min_amount: Decimal
    min_notional: Decimal
    raw: Dict[str, Any]

    @classmethod
    def from_payload(cls, payload: Dict[str, Any]) -> "MarketDefinition":
        if payload.get("schema") != MARKET_DEFINITION_SCHEMA:
            raise ValueError("unsupported 2Finance market definition schema")
        limits = dict(payload.get("limits") or {})
        amount_limits = dict(limits.get("amount") or {})
        notional_limits = dict(limits.get("notional") or limits.get("cost") or {})
        endpoints = dict(payload.get("endpoints") or {})
        definition = cls(
            market_id=str(payload["market_id"]),
            symbol=str(payload["symbol"]),
            engine_id=str(payload["engine_id"]),
            symbol_id=int(payload["symbol_id"]),
            route_epoch=int(payload["route_epoch"]),
            status=str(payload["status"]),
            websocket_endpoint=str(endpoints["websocket"]),
            tick_size=Decimal(str(payload["tick_size"])),
            step_size=Decimal(str(payload["step_size"])),
            min_amount=Decimal(str(amount_limits.get("min") or "0")),
            min_notional=Decimal(str(notional_limits.get("min") or "0")),
            raw=dict(payload),
        )
        if definition.symbol_id <= 0 or definition.route_epoch <= 0:
            raise ValueError("symbol_id and route_epoch must be positive")
        return definition


def parse_market_directory(payload: Dict[str, Any]) -> List[MarketDefinition]:
    if payload.get("schema") != MARKET_DIRECTORY_SCHEMA:
        raise ValueError("unsupported 2Finance market directory schema")
    definitions = [MarketDefinition.from_payload(dict(item)) for item in payload.get("markets", [])]
    engines = {item.engine_id for item in definitions}
    market_ids = {item.market_id for item in definitions}
    if len(engines) != len(definitions):
        raise ValueError("invalid Market Directory: one engine controls multiple symbols")
    if len(market_ids) != len(definitions):
        raise ValueError("invalid Market Directory: duplicate market_id")
    return definitions

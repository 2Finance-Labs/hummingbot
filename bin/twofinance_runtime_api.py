#!/usr/bin/env python3
from __future__ import annotations

import asyncio
import json
import os
import sys
import time
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable

# Executing a module from ``bin`` puts that directory first on sys.path, where
# bin/hummingbot.py would otherwise shadow the actual hummingbot package.
SCRIPT_DIRECTORY = Path(__file__).resolve().parent
REPOSITORY_ROOT = str(SCRIPT_DIRECTORY.parent)
sys.path = [entry for entry in sys.path if Path(entry or ".").resolve() != SCRIPT_DIRECTORY]
sys.path.insert(0, REPOSITORY_ROOT)

from aiohttp import web  # noqa: E402

from hummingbot.connector.exchange.twofinance import twofinance_constants as CONSTANTS  # noqa: E402
from hummingbot.connector.exchange.twofinance.twofinance_exchange import TwoFinanceExchange  # noqa: E402
from hummingbot.connector.exchange.twofinance.twofinance_matchengine_schemas import MatchEngineEvent  # noqa: E402
from hummingbot.core.data_type.common import OrderType, TradeType  # noqa: E402
from hummingbot.core.data_type.in_flight_order import InFlightOrder  # noqa: E402
from hummingbot.twofinance_runtime_observability import (  # noqa: E402
    METRICS,
    MetricsRegistry,
    bounded_method,
    bounded_route,
    configure_telemetry,
    observe_runtime,
    observe_tool,
    request_id,
    server_span,
    set_span_result,
    span_headers,
    structured_http_log,
)

RUNTIME_KEY = web.AppKey("runtime", object)


def env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def normalize_pair(value: str) -> str:
    return value.replace("/", "-")


def json_result(data: Any, *, status: int = 200) -> web.Response:
    return web.json_response({"data": data}, status=status)


@dataclass
class BotHandle:
    config: dict[str, Any]
    status: str = "provisioned"
    exchange: TwoFinanceExchange | None = None
    client_order_id: str | None = None
    exchange_order_id: str | None = None
    last_error: str | None = None
    market_snapshot: dict[str, Any] = field(default_factory=dict)
    started_at: float | None = None
    stopped_at: float | None = None
    user_stream_source: Any | None = field(default=None, repr=False)
    background_tasks: list[asyncio.Task] = field(default_factory=list, repr=False)

    def payload(self) -> dict[str, Any]:
        return {
            "ok": self.last_error is None,
            "bot_name": self.config.get("bot_name"),
            "robot_id": self.config.get("robot_id"),
            "connector_name": self.config.get("connector_name"),
            "engine_id": self.config.get("engine_id"),
            "markets": self.config.get("markets", []),
            "mode": self.config.get("mode"),
            "status": self.status,
            "client_order_id": self.client_order_id,
            "exchange_order_id": self.exchange_order_id,
            "last_error": self.last_error,
            "market_snapshot": self.market_snapshot,
            "started_at": self.started_at,
            "stopped_at": self.stopped_at,
        }


class TwoFinanceBotRuntime:
    def __init__(
        self,
        *,
        exchange_factory: Callable[..., TwoFinanceExchange] = TwoFinanceExchange,
        state_path: str | None = None,
    ) -> None:
        self.exchange_factory = exchange_factory
        self.state_path = Path(state_path or os.getenv("TWO_FINANCE_RUNTIME_STATE", "/data/bots.json"))
        self.bots: dict[str, BotHandle] = {}
        self._locks: dict[str, asyncio.Lock] = {}
        self._load()

    def _load(self) -> None:
        try:
            payload = json.loads(self.state_path.read_text(encoding="utf-8"))
        except (FileNotFoundError, json.JSONDecodeError, OSError):
            return
        for config in payload.get("bots", []):
            if isinstance(config, dict) and config.get("bot_name"):
                self.bots[str(config["bot_name"])] = BotHandle(config=config)

    def _save(self) -> None:
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.state_path.with_suffix(".tmp")
        temporary.write_text(
            json.dumps({"bots": [handle.config for handle in self.bots.values()]}, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        temporary.replace(self.state_path)

    async def provision(self, config: dict[str, Any]) -> dict[str, Any]:
        bot_name = str(config.get("bot_name") or "").strip()
        if not bot_name:
            raise ValueError("bot_name is required")
        connector_name = str(config.get("connector_name") or "twofinance").strip()
        if connector_name not in {"twofinance", "twofinance_testnet"}:
            raise ValueError(f"unsupported connector_name: {connector_name}")
        markets = config.get("markets")
        if not isinstance(markets, list) or not markets:
            raise ValueError("at least one market is required")
        config = {**config, "connector_name": connector_name}
        previous = self.bots.get(bot_name)
        if previous is not None and previous.status == "running":
            raise ValueError(f"cannot reprovision running bot: {bot_name}")
        self.bots[bot_name] = BotHandle(config=config)
        self._save()
        return self.bots[bot_name].payload()

    async def start(self, payload: dict[str, Any]) -> dict[str, Any]:
        bot_name = str(payload.get("bot_name") or "").strip()
        handle = self._require_bot(bot_name)
        async with self._locks.setdefault(bot_name, asyncio.Lock()):
            if handle.status == "running":
                return handle.payload()
            if handle.config.get("mode") == "live" and not env_bool("TWO_FINANCE_RUNTIME_ALLOW_LIVE"):
                raise ValueError("live bot start is disabled by TWO_FINANCE_RUNTIME_ALLOW_LIVE")
            try:
                await self._bootstrap_bot(handle)
            except Exception as exc:
                handle.status = "error"
                handle.last_error = str(exc)
                if handle.exchange is not None and handle.exchange_order_id not in (None, ""):
                    try:
                        await self._cancel_open_order(handle)
                    except Exception as cancel_error:
                        handle.last_error = f"{exc}; compensating cancel failed: {cancel_error}"
                await self._close_exchange(handle)
                raise
            return handle.payload()

    async def stop(self, payload: dict[str, Any], *, archive: bool = False) -> dict[str, Any]:
        bot_name = str(payload.get("bot_name") or "").strip()
        handle = self._require_bot(bot_name)
        async with self._locks.setdefault(bot_name, asyncio.Lock()):
            try:
                await self._cancel_open_order(handle)
                handle.status = "archived" if archive else "stopped"
                handle.stopped_at = time.time()
                handle.last_error = None
            except Exception as exc:
                handle.status = "error"
                handle.last_error = str(exc)
                raise
            finally:
                await self._close_exchange(handle)
            return handle.payload()

    async def _bootstrap_bot(self, handle: BotHandle) -> None:
        config = handle.config
        params = config.get("parameters") if isinstance(config.get("parameters"), dict) else {}
        market = normalize_pair(str(config["markets"][0]))
        engine_id_value = config.get("engine_id")
        if not engine_id_value:
            engine_id_value = os.getenv(
                "TWO_FINANCE_MATCHENGINE_ENGINE_ID", "matchengine-local-v2"
            )
        engine_id = str(engine_id_value)
        exchange = self.exchange_factory(
            twofinance_matchengine_bearer_token=os.getenv("TWO_FINANCE_MATCHENGINE_BEARER_TOKEN", ""),
            twofinance_engine_id=engine_id,
            twofinance_wallet_id=int(params.get("wallet_id") or os.getenv("TWO_FINANCE_MATCHENGINE_WALLET_ID", "1")),
            twofinance_account_id=str(config.get("account_name") or ""),
            twofinance_state_api_url=os.getenv("TWO_FINANCE_STATE_API_URL", "http://matchengine-state-api:8080/api/v2"),
            twofinance_matchengine_ws_url=os.getenv("TWO_FINANCE_MATCHENGINE_WS_URL", "ws://matchengine:10000"),
            trading_pairs=[market],
            trading_required=True,
            ack_timeout=float(os.getenv("TWO_FINANCE_MATCHENGINE_ACK_TIMEOUT", "5")),
        )
        handle.exchange = exchange
        exchange._set_current_timestamp(time.time())

        symbols = await exchange._api_get(path_url=CONSTANTS.SYMBOLS_PATH_URL)
        exchange._initialize_trading_pair_symbols_from_exchange_info(symbols)
        rules = await exchange._api_get(path_url=CONSTANTS.TRADING_RULES_PATH_URL)
        await exchange._update_balances()
        order_book = await exchange._create_order_book_data_source()._order_book_snapshot(market)
        await self._start_user_stream(handle)

        amount = Decimal(str(params.get("order_amount") or os.getenv("TWO_FINANCE_RUNTIME_ORDER_AMOUNT", "1")))
        price = Decimal(str(params.get("order_price") or os.getenv("TWO_FINANCE_RUNTIME_ORDER_PRICE", "1")))
        max_notional = (config.get("risk_policy") or {}).get("max_order_notional")
        if max_notional not in (None, "") and amount * price > Decimal(str(max_notional)):
            raise ValueError("runtime probe order exceeds max_order_notional")
        client_order_id = f"HBOT2F{int(time.time() * 1000)}"
        exchange_order_id, _ = await exchange._place_order(
            order_id=client_order_id,
            trading_pair=market,
            amount=amount,
            trade_type=TradeType.BUY,
            order_type=OrderType.LIMIT,
            price=price,
        )
        handle.client_order_id = client_order_id
        handle.exchange_order_id = str(exchange_order_id)
        handle.market_snapshot = {
            "symbols_ok": bool(symbols),
            "rules_ok": bool(rules),
            "balances": {key: str(value) for key, value in exchange.get_all_balances().items()},
            "best_bid": self._best_price(order_book, "bids"),
            "best_ask": self._best_price(order_book, "asks"),
        }
        handle.status = "running"
        handle.started_at = time.time()
        handle.stopped_at = None
        handle.last_error = None

    async def _cancel_open_order(self, handle: BotHandle) -> None:
        if handle.exchange is None or handle.client_order_id is None or handle.exchange_order_id is None:
            return
        config = handle.config
        params = config.get("parameters") if isinstance(config.get("parameters"), dict) else {}
        market = normalize_pair(str(config["markets"][0]))
        amount = Decimal(str(params.get("order_amount") or os.getenv("TWO_FINANCE_RUNTIME_ORDER_AMOUNT", "1")))
        price = Decimal(str(params.get("order_price") or os.getenv("TWO_FINANCE_RUNTIME_ORDER_PRICE", "1")))
        tracked = InFlightOrder(
            client_order_id=handle.client_order_id,
            trading_pair=market,
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            amount=amount,
            price=price,
            exchange_order_id=handle.exchange_order_id,
            creation_timestamp=handle.started_at or time.time(),
        )
        cancel_id = f"{handle.client_order_id}C"
        # The MatchEngine closes idle command sockets. A bot may legitimately
        # stay running longer than that idle window, so never reuse the ADD
        # socket for the safety-critical DELETE command.
        await handle.exchange._matchengine_client.close()
        await handle.exchange._place_cancel(cancel_id, tracked)
        response = await handle.exchange._matchengine_client.wait_for_ack(
            cancel_id, float(os.getenv("TWO_FINANCE_MATCHENGINE_ACK_TIMEOUT", "5"))
        )
        if response is not None and not response.accepted:
            raise RuntimeError(response.reason or "MatchEngine rejected cancel")

    async def _close_exchange(self, handle: BotHandle) -> None:
        if handle.exchange is None:
            return
        for task in handle.background_tasks:
            task.cancel()
        if handle.background_tasks:
            await asyncio.gather(*handle.background_tasks, return_exceptions=True)
        handle.background_tasks.clear()
        if handle.user_stream_source is not None:
            await handle.user_stream_source.stop()
            handle.user_stream_source = None
        await handle.exchange._matchengine_client.close()
        await handle.exchange._web_assistants_factory.close()
        handle.exchange = None

    async def _start_user_stream(self, handle: BotHandle) -> None:
        if handle.exchange is None:
            raise RuntimeError("exchange is not initialized")
        queue: asyncio.Queue = asyncio.Queue()
        source = handle.exchange._create_user_stream_data_source()
        handle.user_stream_source = source

        async def apply_events() -> None:
            while True:
                payload = await queue.get()
                if not isinstance(payload, dict):
                    continue
                event = MatchEngineEvent.from_payload(payload)
                if event.event_type:
                    handle.exchange._matchengine_client.apply_event(event)

        handle.background_tasks = [
            asyncio.create_task(source.listen_for_user_stream(queue)),
            asyncio.create_task(apply_events()),
        ]
        for _ in range(30):
            if source._ws_assistant is not None:
                await asyncio.sleep(0.2)
                return
            await asyncio.sleep(0.1)
        raise RuntimeError("timed out subscribing to MatchEngine wallet events")

    @staticmethod
    def _best_price(order_book: Any, side: str) -> str | None:
        content = getattr(order_book, "content", {})
        levels = content.get(side, []) if isinstance(content, dict) else []
        if not levels:
            return None
        level = levels[0]
        if isinstance(level, dict):
            value = level.get("price")
        elif isinstance(level, (list, tuple)) and level:
            value = level[0]
        else:
            value = None
        return str(value) if value is not None else None

    def _require_bot(self, bot_name: str) -> BotHandle:
        if not bot_name or bot_name not in self.bots:
            raise KeyError(f"bot not found: {bot_name}")
        return self.bots[bot_name]


def mcp_tool(name: str, description: str, properties: dict[str, Any] | None = None) -> dict[str, Any]:
    return {
        "name": name,
        "description": description,
        "inputSchema": {"type": "object", "properties": properties or {}},
    }


def create_app(
    runtime: TwoFinanceBotRuntime | None = None,
    metrics: MetricsRegistry | None = None,
) -> web.Application:
    runtime = runtime or TwoFinanceBotRuntime()
    metrics = metrics or METRICS
    app = web.Application()
    app[RUNTIME_KEY] = runtime

    async def health(_: web.Request) -> web.Response:
        return web.json_response({"ok": True, "runtime": "hummingbot-twofinance", "bots": len(runtime.bots)})

    async def list_bots(_: web.Request) -> web.Response:
        return json_result({"bots": {name: handle.payload() for name, handle in runtime.bots.items()}})

    async def bot_status(request: web.Request) -> web.Response:
        return json_result(runtime._require_bot(request.match_info["bot_name"]).payload())

    async def provision(request: web.Request) -> web.Response:
        payload = await request.json()
        return json_result(
            await observe_runtime(metrics, "provision", lambda: runtime.provision(payload))
        )

    async def start(request: web.Request) -> web.Response:
        payload = await request.json()
        return json_result(await observe_runtime(metrics, "start", lambda: runtime.start(payload)))

    async def stop(request: web.Request) -> web.Response:
        payload = await request.json()
        return json_result(await observe_runtime(metrics, "stop", lambda: runtime.stop(payload)))

    async def archive(request: web.Request) -> web.Response:
        return json_result(
            await observe_runtime(
                metrics,
                "archive",
                lambda: runtime.stop({"bot_name": request.match_info["bot_name"]}, archive=True),
            )
        )

    async def prometheus(_: web.Request) -> web.Response:
        return web.Response(text=metrics.prometheus(), content_type="text/plain", charset="utf-8")

    async def mcp(request: web.Request) -> web.Response:
        body = await request.json()
        request_id = body.get("id")
        method = body.get("method")
        if method == "initialize":
            result: dict[str, Any] = {
                "protocolVersion": "2025-03-26",
                "capabilities": {"tools": {}},
                "serverInfo": {"name": "2finance-hummingbot-mcp", "version": "1.0.0"},
            }
        elif method == "tools/list":
            result = {"tools": [
                mcp_tool("bot.list", "List provisioned Hummingbot bots."),
                mcp_tool("bot.status", "Read one Hummingbot bot status.", {"bot_name": {"type": "string"}}),
                mcp_tool(
                    "market.data",
                    "Read the latest market snapshot captured by a bot.",
                    {"bot_name": {"type": "string"}},
                ),
            ]}
        elif method == "tools/call":
            params = body.get("params") if isinstance(body.get("params"), dict) else {}
            arguments = params.get("arguments") if isinstance(params.get("arguments"), dict) else {}
            name = str(params.get("name") or "")

            async def call_tool() -> Any:
                if name == "bot.list":
                    return {key: item.payload() for key, item in runtime.bots.items()}
                if name == "bot.status":
                    return runtime._require_bot(str(arguments.get("bot_name") or "")).payload()
                if name == "market.data":
                    return runtime._require_bot(str(arguments.get("bot_name") or "")).market_snapshot
                raise web.HTTPBadRequest(text=f"unknown MCP tool: {name}")

            value = await observe_tool(metrics, name, call_tool)
            result = {
                "content": [
                    {"type": "text", "text": json.dumps(value, sort_keys=True)}
                ],
                "structuredContent": value,
            }
        else:
            return web.json_response(
                {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "error": {"code": -32601, "message": "method not found"},
                }
            )
        return web.json_response({"jsonrpc": "2.0", "id": request_id, "result": result})

    @web.middleware
    async def errors(request: web.Request, handler: Any) -> web.StreamResponse:
        try:
            return await handler(request)
        except web.HTTPException:
            raise
        except KeyError as exc:
            return web.json_response({"detail": str(exc)}, status=404)
        except (ValueError, RuntimeError) as exc:
            return web.json_response({"detail": str(exc)}, status=422)
        except Exception:
            return web.json_response({"detail": "internal server error"}, status=500)

    @web.middleware
    async def observability(request: web.Request, handler: Any) -> web.StreamResponse:
        started = time.monotonic()
        method = bounded_method(request.method)
        route = bounded_route(request.path)
        correlation_id = request_id(request.headers.get("X-Request-ID"))
        with server_span(dict(request.headers), route, method) as span:
            raised: web.HTTPException | None = None
            try:
                response = await handler(request)
            except web.HTTPException as exc:
                response = exc
                raised = exc
            duration_ms = int((time.monotonic() - started) * 1000)
            metrics.observe_http(method, route, response.status, time.monotonic() - started)
            set_span_result(span, response.status)
            response.headers["X-Request-ID"] = correlation_id
            for name, value in span_headers(span).items():
                response.headers[name] = value
            structured_http_log(
                correlation_id=correlation_id,
                method=method,
                route=route,
                status=response.status,
                duration_ms=duration_ms,
                span=span,
            )
            if raised is not None:
                raise raised
            return response

    app.middlewares.extend((observability, errors))
    app.router.add_get("/health", health)
    app.router.add_get("/healthz", health)
    app.router.add_get("/readyz", health)
    app.router.add_get("/metrics", prometheus)
    app.router.add_get("/bot-orchestration/status", list_bots)
    app.router.add_get("/bot-orchestration/{bot_name}/status", bot_status)
    app.router.add_post("/bot-orchestration/provision-bot", provision)
    app.router.add_post("/bot-orchestration/start-bot", start)
    app.router.add_post("/bot-orchestration/stop-bot", stop)
    app.router.add_post("/bot-orchestration/stop-and-archive-bot/{bot_name}", archive)
    app.router.add_post("/mcp", mcp)
    return app


def main() -> None:
    try:
        telemetry = configure_telemetry()
    except Exception:
        print("OpenTelemetry exporter unavailable; continuing fail-open", flush=True)
        telemetry = None
    try:
        web.run_app(
            create_app(),
            host=os.getenv("TWO_FINANCE_RUNTIME_HOST", "0.0.0.0"),
            port=int(os.getenv("TWO_FINANCE_RUNTIME_PORT", "8000")),
        )
    finally:
        if telemetry is not None:
            telemetry.shutdown()


if __name__ == "__main__":
    main()

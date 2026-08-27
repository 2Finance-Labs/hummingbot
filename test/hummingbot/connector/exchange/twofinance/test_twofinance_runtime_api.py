import importlib.util
import os
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from aiohttp.test_utils import TestClient, TestServer

MODULE_PATH = Path(__file__).parents[5] / "bin" / "twofinance_runtime_api.py"
SPEC = importlib.util.spec_from_file_location("twofinance_runtime_api", MODULE_PATH)
runtime_api = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = runtime_api
SPEC.loader.exec_module(runtime_api)


def config(mode="dry_run"):
    return {
        "schema": "hummingbot_bot_config.v1",
        "robot_id": "robot-1",
        "bot_name": "hbot-local-1",
        "connector_name": "twofinance",
        "engine_id": "matchengine-local-v2",
        "markets": ["BTC/USDT"],
        "mode": mode,
        "risk_policy": {"max_order_notional": "10"},
        "parameters": {"order_amount": "1", "order_price": "1"},
    }


class TwoFinanceRuntimeTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.runtime = runtime_api.TwoFinanceBotRuntime(
            state_path=str(Path(self.temporary.name) / "bots.json")
        )

    async def asyncTearDown(self):
        self.temporary.cleanup()

    async def test_provision_persists_native_twofinance_config(self):
        result = await self.runtime.provision(config())
        self.assertTrue(result["ok"])
        self.assertEqual(result["connector_name"], "twofinance")

        restored = runtime_api.TwoFinanceBotRuntime(state_path=str(self.runtime.state_path))
        self.assertIn("hbot-local-1", restored.bots)
        self.assertEqual(restored.bots["hbot-local-1"].config["engine_id"], "matchengine-local-v2")

    async def test_live_start_is_fail_closed_without_runtime_gate(self):
        await self.runtime.provision(config(mode="live"))
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("TWO_FINANCE_RUNTIME_ALLOW_LIVE", None)
            with self.assertRaisesRegex(ValueError, "live bot start is disabled"):
                await self.runtime.start({"bot_name": "hbot-local-1"})

    async def test_http_lifecycle_and_mcp_discovery(self):
        client = TestClient(TestServer(runtime_api.create_app(self.runtime)))
        await client.start_server()
        try:
            response = await client.post("/bot-orchestration/provision-bot", json=config())
            self.assertEqual(response.status, 200)
            self.assertEqual((await response.json())["data"]["status"], "provisioned")

            response = await client.get("/bot-orchestration/status")
            self.assertIn("hbot-local-1", (await response.json())["data"]["bots"])

            response = await client.post(
                "/mcp",
                json={"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}},
            )
            names = {item["name"] for item in (await response.json())["result"]["tools"]}
            self.assertEqual(names, {"bot.list", "bot.status", "market.data"})
        finally:
            await client.close()

    def test_best_price_reads_order_book_message_content(self):
        message = SimpleNamespace(content={"bids": [["99.50", "1"]], "asks": [["100.50", "1"]]})

        self.assertEqual(self.runtime._best_price(message, "bids"), "99.50")
        self.assertEqual(self.runtime._best_price(message, "asks"), "100.50")

    async def test_cancel_reconnects_command_socket_before_delete(self):
        events = []

        async def close():
            events.append("close")

        async def place_cancel(*_args):
            events.append("delete")

        client = SimpleNamespace(
            close=AsyncMock(side_effect=close),
            wait_for_ack=AsyncMock(return_value=SimpleNamespace(accepted=True)),
        )
        exchange = SimpleNamespace(
            _matchengine_client=client,
            _place_cancel=AsyncMock(side_effect=place_cancel),
        )
        handle = runtime_api.BotHandle(
            config=config(),
            exchange=exchange,
            client_order_id="HBOT-ORDER-1",
            exchange_order_id="14",
        )

        await self.runtime._cancel_open_order(handle)

        self.assertEqual(events, ["close", "delete"])
        client.wait_for_ack.assert_awaited_once()


if __name__ == "__main__":
    unittest.main()

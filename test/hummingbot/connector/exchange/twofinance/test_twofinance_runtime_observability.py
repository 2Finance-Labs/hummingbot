from __future__ import annotations

import importlib.util
import io
import json
import os
import subprocess
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

from aiohttp.test_utils import TestClient, TestServer

from hummingbot.twofinance_runtime_observability import (
    MetricsRegistry,
    bounded_method,
    bounded_route,
    bounded_tool_operation,
    export_timeout,
    observe_runtime,
    sample_ratio,
    signal_endpoint,
    validate_endpoint,
)

MODULE_PATH = Path(__file__).parents[5] / "bin" / "twofinance_runtime_api.py"
SPEC = importlib.util.spec_from_file_location("twofinance_runtime_api_observability_test", MODULE_PATH)
runtime_api = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = runtime_api
SPEC.loader.exec_module(runtime_api)

try:
    OTEL_SDK_INSTALLED = importlib.util.find_spec("opentelemetry.sdk") is not None
except ModuleNotFoundError:
    OTEL_SDK_INSTALLED = False


def bot_config() -> dict:
    return {
        "schema": "hummingbot_bot_config.v1",
        "robot_id": "private-robot-id",
        "bot_name": "private-bot-name",
        "connector_name": "twofinance",
        "engine_id": "private-engine-id",
        "markets": ["PRIVATE/USDT"],
        "mode": "dry_run",
        "risk_policy": {"max_order_notional": "10"},
        "parameters": {"order_amount": "1", "order_price": "1"},
    }


class TwoFinanceRuntimeObservabilityTests(unittest.IsolatedAsyncioTestCase):
    async def test_http_mcp_and_runtime_metrics_are_bounded_and_privacy_safe(self) -> None:
        metrics = MetricsRegistry()
        output = io.StringIO()
        with tempfile.TemporaryDirectory() as temporary:
            runtime = runtime_api.TwoFinanceBotRuntime(
                state_path=str(Path(temporary) / "bots.json")
            )
            client = TestClient(TestServer(runtime_api.create_app(runtime, metrics)))
            await client.start_server()
            try:
                with redirect_stdout(output):
                    health = await client.get(
                        "/healthz?token=private-query-token",
                        headers={"X-Request-ID": "request-safe-1"},
                    )
                    self.assertEqual(health.status, 200)
                    self.assertEqual(health.headers["X-Request-ID"], "request-safe-1")

                    provisioned = await client.post(
                        "/bot-orchestration/provision-bot", json=bot_config()
                    )
                    self.assertEqual(provisioned.status, 200)

                    listed = await client.post(
                        "/mcp",
                        json={
                            "jsonrpc": "2.0",
                            "id": 1,
                            "method": "tools/call",
                            "params": {"name": "bot.list", "arguments": {}},
                        },
                    )
                    self.assertEqual(listed.status, 200)

                    unknown = await client.post(
                        "/mcp",
                        json={
                            "jsonrpc": "2.0",
                            "id": 2,
                            "method": "tools/call",
                            "params": {
                                "name": "private.tool.name",
                                "arguments": {"token": "private-argument-token"},
                            },
                        },
                        headers={"X-Request-ID": "invalid request id"},
                    )
                    self.assertEqual(unknown.status, 400)
                    self.assertRegex(unknown.headers["X-Request-ID"], r"^req_[0-9a-f]{32}$")

                    missing = await client.get(
                        "/bot-orchestration/private-missing-bot/status"
                    )
                    self.assertEqual(missing.status, 404)

                    response = await client.get("/metrics")
                    self.assertEqual(response.status, 200)
                    prometheus = await response.text()
            finally:
                await client.close()

        logs = output.getvalue()
        self.assertIn('"route":"/healthz"', logs)
        self.assertIn('"route":"/bot-orchestration/{id}/status"', logs)
        self.assertIn('operation="bot_list",outcome="success"', prometheus)
        self.assertIn('operation="dynamic",outcome="failed"', prometheus)
        self.assertIn('operation="provision",outcome="success"', prometheus)
        self.assertIn('route="/bot-orchestration/{id}/status"', prometheus)
        for private_value in (
            "private-query-token",
            "private-robot-id",
            "private-bot-name",
            "private-engine-id",
            "PRIVATE/USDT",
            "private.tool.name",
            "private-argument-token",
            "private-missing-bot",
            "invalid request id",
        ):
            self.assertNotIn(private_value, logs)
            self.assertNotIn(private_value, prometheus)

    async def test_unexpected_runtime_failure_is_internal_and_does_not_leak(self) -> None:
        metrics = MetricsRegistry()

        async def fail() -> None:
            raise Exception("private-runtime-error")

        with self.assertRaisesRegex(Exception, "private-runtime-error"):
            await observe_runtime(metrics, "start", fail)
        payload = metrics.prometheus()
        self.assertIn('operation="start",outcome="internal_failure"', payload)
        self.assertNotIn("private-runtime-error", payload)

    def test_dimensions_and_otlp_configuration_are_bounded(self) -> None:
        self.assertEqual(bounded_method("BREW"), "OTHER")
        self.assertEqual(
            bounded_route("/bot-orchestration/private-bot/status"),
            "/bot-orchestration/{id}/status",
        )
        self.assertEqual(bounded_route("/private/path"), "unmatched")
        self.assertEqual(bounded_tool_operation("private.tool.name"), "dynamic")
        secret_endpoint = "https://private-user:private-password@collector.example/v1/traces"
        with self.assertRaisesRegex(ValueError, "endpoint is invalid") as error:
            validate_endpoint(secret_endpoint)
        self.assertNotIn("private-user", str(error.exception))
        self.assertNotIn("private-password", str(error.exception))
        self.assertEqual(
            signal_endpoint("http://collector:4318", "traces"),
            "http://collector:4318/v1/traces",
        )
        with patch.dict(
            os.environ,
            {"OCTO_OTEL_SAMPLE_RATIO": "0.25", "OTEL_EXPORTER_OTLP_TIMEOUT": "7s"},
        ):
            self.assertEqual(sample_ratio(), 0.25)
            self.assertEqual(export_timeout(), 7.0)

    @unittest.skipUnless(OTEL_SDK_INSTALLED, "OpenTelemetry SDK is not installed")
    def test_real_otlp_export_preserves_parent_and_excludes_request_secret(self) -> None:
        script = r'''
import json
import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

requests = []
class Receiver(BaseHTTPRequestHandler):
    def do_POST(self):
        body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
        requests.append((self.path, body))
        self.send_response(200)
        self.send_header("Content-Length", "0")
        self.end_headers()
    def log_message(self, *_args):
        return

receiver = ThreadingHTTPServer(("127.0.0.1", 0), Receiver)
thread = threading.Thread(target=receiver.serve_forever, daemon=True)
thread.start()
os.environ["OTEL_EXPORTER_OTLP_ENDPOINT"] = f"http://127.0.0.1:{receiver.server_address[1]}"
os.environ["OCTO_OTEL_SAMPLE_RATIO"] = "1"

from hummingbot.twofinance_runtime_observability import configure_telemetry, server_span
runtime = configure_telemetry()
with server_span(
    {
        "traceparent": "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01",
        "Authorization": "Bearer private-request-secret",
    },
    "/bot-orchestration/{id}/status",
    "GET",
) as span:
    parent_span_id = span.parent.span_id if span.parent is not None else 0
runtime.tracer_provider.force_flush()
runtime.meter_provider.force_flush()
runtime.shutdown()
receiver.shutdown()
thread.join(timeout=5)
receiver.server_close()
body = b"".join(item[1] for item in requests)
print(json.dumps({
    "paths": sorted(set(item[0] for item in requests)),
    "parent_span_id": format(parent_span_id, "016x"),
    "leaked": b"private-request-secret" in body,
}))
'''
        env = dict(os.environ)
        env["PYTHONPATH"] = str(Path(__file__).parents[5]) + os.pathsep + env.get(
            "PYTHONPATH", ""
        )
        process = subprocess.run(
            [sys.executable, "-c", script],
            env=env,
            check=True,
            capture_output=True,
            text=True,
            timeout=30,
        )
        result = json.loads(process.stdout.strip().splitlines()[-1])
        self.assertEqual(result["parent_span_id"], "0123456789abcdef")
        self.assertIn("/v1/traces", result["paths"])
        self.assertIn("/v1/metrics", result["paths"])
        self.assertFalse(result["leaked"])


if __name__ == "__main__":
    unittest.main()

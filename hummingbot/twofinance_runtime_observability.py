from __future__ import annotations

import json
import os
import secrets
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Iterator, Mapping, TypeVar
from urllib.parse import urlparse

SERVICE_NAME = "hummingbot-runtime"
MCP_SERVER_ID = "hummingbot"
DURATION_BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0)
STANDARD_METHODS = {"CONNECT", "DELETE", "GET", "HEAD", "OPTIONS", "PATCH", "POST", "PUT", "TRACE"}
TOOL_OPERATIONS = {"bot_list", "bot_status", "market_data", "dynamic"}
RUNTIME_OPERATIONS = {"provision", "start", "stop", "archive"}
OUTCOMES = {"success", "failed", "denied", "internal_failure"}
T = TypeVar("T")


@dataclass
class HistogramValue:
    count: int = 0
    total: float = 0.0
    buckets: list[int] = field(default_factory=lambda: [0] * len(DURATION_BUCKETS))

    def observe(self, seconds: float) -> None:
        seconds = max(0.0, seconds)
        self.count += 1
        self.total += seconds
        for index, bucket in enumerate(DURATION_BUCKETS):
            if seconds <= bucket:
                self.buckets[index] += 1


class MetricsRegistry:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._http: dict[tuple[str, str, str], HistogramValue] = {}
        self._tools: dict[tuple[str, str], HistogramValue] = {}
        self._runtime: dict[tuple[str, str], HistogramValue] = {}

    def observe_http(self, method: str, route: str, status: int, seconds: float) -> None:
        key = (bounded_method(method), bounded_route(route), bounded_status(status))
        with self._lock:
            self._http.setdefault(key, HistogramValue()).observe(seconds)

    def observe_tool(self, operation: str, outcome: str, seconds: float) -> None:
        key = (bounded_tool_operation(operation), bounded_outcome(outcome))
        with self._lock:
            self._tools.setdefault(key, HistogramValue()).observe(seconds)

    def observe_runtime(self, operation: str, outcome: str, seconds: float) -> None:
        key = (bounded_runtime_operation(operation), bounded_outcome(outcome))
        with self._lock:
            self._runtime.setdefault(key, HistogramValue()).observe(seconds)

    def prometheus(self) -> str:
        with self._lock:
            http_values = {key: copy_histogram(value) for key, value in self._http.items()}
            tool_values = {key: copy_histogram(value) for key, value in self._tools.items()}
            runtime_values = {key: copy_histogram(value) for key, value in self._runtime.items()}
        lines = [
            "# HELP mcp_server_build_info Static MCP server identity.",
            "# TYPE mcp_server_build_info gauge",
            f'mcp_server_build_info{{service="{SERVICE_NAME}",component="mcp-server",mcp_server="{MCP_SERVER_ID}"}} 1',
            "# HELP http_requests_total HTTP requests handled by the Hummingbot runtime.",
            "# TYPE http_requests_total counter",
        ]
        for (method, route, status), value in sorted(http_values.items()):
            labels = http_labels(method, route, status)
            lines.append(f"http_requests_total{{{labels}}} {value.count}")
        lines.extend(
            [
                "# HELP http_request_duration_seconds HTTP request duration by normalized route.",
                "# TYPE http_request_duration_seconds histogram",
            ]
        )
        for (method, route, status), value in sorted(http_values.items()):
            write_histogram(lines, "http_request_duration_seconds", http_labels(method, route, status), value)
        lines.extend(
            [
                "# HELP octo_mcp_tool_calls_total MCP tool calls by bounded operation and outcome.",
                "# TYPE octo_mcp_tool_calls_total counter",
            ]
        )
        for (operation, outcome), value in sorted(tool_values.items()):
            labels = operation_labels("tools", operation, outcome)
            lines.append(f"octo_mcp_tool_calls_total{{{labels}}} {value.count}")
        lines.extend(
            [
                "# HELP octo_mcp_tool_duration_seconds MCP tool duration by bounded operation and outcome.",
                "# TYPE octo_mcp_tool_duration_seconds histogram",
            ]
        )
        for (operation, outcome), value in sorted(tool_values.items()):
            write_histogram(
                lines,
                "octo_mcp_tool_duration_seconds",
                operation_labels("tools", operation, outcome),
                value,
            )
        lines.extend(
            [
                "# HELP octo_hummingbot_runtime_operations_total Hummingbot lifecycle operations by bounded outcome.",
                "# TYPE octo_hummingbot_runtime_operations_total counter",
            ]
        )
        for (operation, outcome), value in sorted(runtime_values.items()):
            labels = runtime_labels(operation, outcome)
            lines.append(f"octo_hummingbot_runtime_operations_total{{{labels}}} {value.count}")
        lines.extend(
            [
                "# HELP octo_hummingbot_runtime_operation_duration_seconds Hummingbot lifecycle operation duration.",
                "# TYPE octo_hummingbot_runtime_operation_duration_seconds histogram",
            ]
        )
        for (operation, outcome), value in sorted(runtime_values.items()):
            write_histogram(
                lines,
                "octo_hummingbot_runtime_operation_duration_seconds",
                runtime_labels(operation, outcome),
                value,
            )
        return "\n".join(lines) + "\n"


def copy_histogram(value: HistogramValue) -> HistogramValue:
    return HistogramValue(count=value.count, total=value.total, buckets=list(value.buckets))


def write_histogram(lines: list[str], name: str, labels: str, value: HistogramValue) -> None:
    for bucket, count in zip(DURATION_BUCKETS, value.buckets):
        lines.append(f'{name}_bucket{{{labels},le="{bucket:g}"}} {count}')
    lines.append(f'{name}_bucket{{{labels},le="+Inf"}} {value.count}')
    lines.append(f"{name}_sum{{{labels}}} {value.total:g}")
    lines.append(f"{name}_count{{{labels}}} {value.count}")


def http_labels(method: str, route: str, status: str) -> str:
    return f'service="{SERVICE_NAME}",component="api",method="{method}",route="{route}",status="{status}"'


def operation_labels(component: str, operation: str, outcome: str) -> str:
    return (
        f'service="{SERVICE_NAME}",component="{component}",mcp_server="{MCP_SERVER_ID}",'
        f'operation="{operation}",outcome="{outcome}"'
    )


def runtime_labels(operation: str, outcome: str) -> str:
    return f'service="{SERVICE_NAME}",component="runtime",operation="{operation}",outcome="{outcome}"'


def bounded_method(method: str) -> str:
    normalized = str(method).strip().upper()
    return normalized if normalized in STANDARD_METHODS else "OTHER"


def bounded_status(status: int) -> str:
    return str(status) if 100 <= int(status) <= 599 else "other"


def bounded_route(path: str) -> str:
    if path in {
        "/health",
        "/healthz",
        "/readyz",
        "/metrics",
        "/mcp",
        "/bot-orchestration/status",
        "/bot-orchestration/provision-bot",
        "/bot-orchestration/start-bot",
        "/bot-orchestration/stop-bot",
    }:
        return path
    if path.startswith("/bot-orchestration/") and path.endswith("/status"):
        return "/bot-orchestration/{id}/status"
    if path.startswith("/bot-orchestration/stop-and-archive-bot/"):
        return "/bot-orchestration/stop-and-archive-bot/{id}"
    return "unmatched"


def bounded_tool_operation(name: str) -> str:
    canonical = str(name).strip().lower()
    if canonical in TOOL_OPERATIONS:
        return canonical
    return {
        "bot.list": "bot_list",
        "bot.status": "bot_status",
        "market.data": "market_data",
    }.get(canonical, "dynamic")


def bounded_runtime_operation(name: str) -> str:
    canonical = str(name).strip().lower()
    return canonical if canonical in RUNTIME_OPERATIONS else "internal"


def bounded_outcome(outcome: str) -> str:
    canonical = str(outcome).strip().lower()
    return canonical if canonical in OUTCOMES else "internal_failure"


def request_id(value: str | None) -> str:
    candidate = str(value or "").strip()
    allowed = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_.:")
    if candidate and len(candidate) <= 128 and all(character in allowed for character in candidate):
        return candidate
    return "req_" + secrets.token_hex(16)


METRICS = MetricsRegistry()


class NoopSpan:
    def set_attribute(self, _name: str, _value: Any) -> None:
        return

    def set_status(self, _status: Any) -> None:
        return

    def get_span_context(self) -> Any:
        return None


@contextmanager
def server_span(carrier: Mapping[str, str], route: str, method: str) -> Iterator[Any]:
    try:
        from opentelemetry import trace
        from opentelemetry.propagators.textmap import Getter
        from opentelemetry.trace import SpanKind
        from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
    except ImportError:
        yield NoopSpan()
        return

    class HeaderGetter(Getter[Mapping[str, str]]):
        def get(self, value: Mapping[str, str], key: str) -> list[str] | None:
            for header_name, header_value in value.items():
                if header_name.lower() == key.lower():
                    return [header_value]
            return None

        def keys(self, value: Mapping[str, str]) -> list[str]:
            return list(value.keys())

    context = TraceContextTextMapPropagator().extract(carrier=carrier, getter=HeaderGetter())
    tracer = trace.get_tracer("2finance-hummingbot-runtime/http")
    with tracer.start_as_current_span(
        route,
        context=context,
        kind=SpanKind.SERVER,
        attributes={"http.request.method": method, "http.route": route},
    ) as span:
        yield span


@contextmanager
def operation_span(name: str, operation: str, component: str) -> Iterator[Any]:
    try:
        from opentelemetry import trace
        from opentelemetry.trace import SpanKind
    except ImportError:
        yield NoopSpan()
        return
    tracer = trace.get_tracer("2finance-hummingbot-runtime/operations")
    attributes = {"octo.operation": operation, "octo.component": component}
    if component == "tools":
        attributes["mcp.server"] = MCP_SERVER_ID
    with tracer.start_as_current_span(name, kind=SpanKind.INTERNAL, attributes=attributes) as span:
        yield span


def set_span_result(span: Any, status: int) -> None:
    span.set_attribute("http.response.status_code", status)
    if status >= 500:
        set_error_status(span)


def set_operation_result(span: Any, outcome: str) -> None:
    span.set_attribute("octo.outcome", bounded_outcome(outcome))
    if outcome in {"failed", "internal_failure"}:
        set_error_status(span)


def set_error_status(span: Any) -> None:
    try:
        from opentelemetry.trace import Status, StatusCode

        span.set_status(Status(StatusCode.ERROR))
    except ImportError:
        return


def span_headers(span: Any) -> dict[str, str]:
    context = span.get_span_context()
    if context is None or not getattr(context, "is_valid", False):
        return {}
    trace_id = format(context.trace_id, "032x")
    span_id = format(context.span_id, "016x")
    flags = int(context.trace_flags) & 0xFF
    return {"X-Trace-ID": trace_id, "traceparent": f"00-{trace_id}-{span_id}-{flags:02x}"}


async def observe_tool(metrics: MetricsRegistry, name: str, callback: Callable[[], Awaitable[T]]) -> T:
    return await observe_operation(metrics, "tools", bounded_tool_operation(name), callback)


async def observe_runtime(metrics: MetricsRegistry, name: str, callback: Callable[[], Awaitable[T]]) -> T:
    return await observe_operation(metrics, "runtime", bounded_runtime_operation(name), callback)


async def observe_operation(
    metrics: MetricsRegistry,
    component: str,
    operation: str,
    callback: Callable[[], Awaitable[T]],
) -> T:
    started = time.monotonic()
    outcome = "success"
    span_name = "mcp.tool" if component == "tools" else "hummingbot.runtime.operation"
    with operation_span(span_name, operation, component) as span:
        try:
            return await callback()
        except (KeyError, ValueError, RuntimeError):
            outcome = "failed"
            raise
        except Exception as error:
            status = int(getattr(error, "status", 500))
            outcome = "failed" if status < 500 else "internal_failure"
            raise
        finally:
            elapsed = time.monotonic() - started
            if component == "tools":
                metrics.observe_tool(operation, outcome, elapsed)
            else:
                metrics.observe_runtime(operation, outcome, elapsed)
            set_operation_result(span, outcome)


def structured_http_log(
    *, correlation_id: str, method: str, route: str, status: int, duration_ms: int, span: Any
) -> None:
    entry: dict[str, Any] = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "level": "error" if status >= 500 else "warning" if status >= 400 else "info",
        "event_name": "http_request",
        "environment": os.getenv("OCTO_ENVIRONMENT", "unknown"),
        "cluster": os.getenv("OCTO_CLUSTER", "unknown"),
        "namespace": os.getenv("POD_NAMESPACE", "unknown"),
        "service": SERVICE_NAME,
        "component": "api",
        "request_id": correlation_id,
        "method": method,
        "route": route,
        "status": status,
        "duration_ms": duration_ms,
    }
    context = span.get_span_context()
    if context is not None and getattr(context, "is_valid", False):
        entry["trace_id"] = format(context.trace_id, "032x")
        entry["span_id"] = format(context.span_id, "016x")
    print(json.dumps(entry, sort_keys=True, separators=(",", ":")), flush=True)


class TelemetryRuntime:
    def __init__(self, tracer_provider: Any = None, meter_provider: Any = None) -> None:
        self.tracer_provider = tracer_provider
        self.meter_provider = meter_provider
        self.stop_event = threading.Event()
        self.thread: threading.Thread | None = None

    def start_heartbeat(self, interval: float = 60.0) -> None:
        try:
            from opentelemetry import metrics, trace
        except ImportError:
            return
        counter = metrics.get_meter("octo/telemetry-heartbeat").create_counter(
            "octo_telemetry_heartbeat", description="Privacy-safe synthetic OTLP metric pipeline heartbeat."
        )
        tracer = trace.get_tracer("octo/telemetry-heartbeat")

        def emit() -> None:
            with tracer.start_as_current_span("octo.telemetry.heartbeat") as span:
                span.set_attribute("octo.signal.kind", "heartbeat")
                span.set_attribute("octo.signal.synthetic", True)
            counter.add(1)

        def run() -> None:
            emit()
            while not self.stop_event.wait(interval):
                emit()

        self.thread = threading.Thread(target=run, name="octo-telemetry-heartbeat", daemon=True)
        self.thread.start()

    def shutdown(self) -> None:
        self.stop_event.set()
        if self.thread is not None:
            self.thread.join(timeout=2.0)
        if self.meter_provider is not None:
            self.meter_provider.shutdown()
        if self.tracer_provider is not None:
            self.tracer_provider.shutdown()


def configure_telemetry() -> TelemetryRuntime:
    endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "").strip()
    trace_endpoint = os.getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "").strip()
    metric_endpoint = os.getenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", "").strip()
    for value in (endpoint, trace_endpoint, metric_endpoint):
        if value:
            validate_endpoint(value)
    if not (endpoint or trace_endpoint or metric_endpoint):
        return TelemetryRuntime()
    try:
        from opentelemetry import metrics, trace
        from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
        from opentelemetry.sdk.metrics import MeterProvider
        from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor
        from opentelemetry.sdk.trace.sampling import ParentBased, TraceIdRatioBased
    except ImportError:
        return TelemetryRuntime()

    resource = Resource.create(
        {
            "service.name": SERVICE_NAME,
            "service.version": os.getenv("OCTO_VERSION", "unknown"),
            "deployment.environment": os.getenv("OCTO_ENVIRONMENT", "unknown"),
            "k8s.cluster.name": os.getenv("OCTO_CLUSTER", "unknown"),
            "k8s.namespace.name": os.getenv("POD_NAMESPACE", "unknown"),
        }
    )
    timeout = export_timeout()
    tracer_provider = TracerProvider(resource=resource, sampler=ParentBased(TraceIdRatioBased(sample_ratio())))
    if endpoint or trace_endpoint:
        exporter = OTLPSpanExporter(endpoint=signal_endpoint(trace_endpoint or endpoint, "traces"), timeout=timeout)
        tracer_provider.add_span_processor(BatchSpanProcessor(exporter))
    trace.set_tracer_provider(tracer_provider)
    readers = []
    if endpoint or metric_endpoint:
        exporter = OTLPMetricExporter(endpoint=signal_endpoint(metric_endpoint or endpoint, "metrics"), timeout=timeout)
        readers.append(
            PeriodicExportingMetricReader(
                exporter,
                export_interval_millis=30_000,
                export_timeout_millis=int(timeout * 1_000),
            )
        )
    meter_provider = MeterProvider(resource=resource, metric_readers=readers)
    metrics.set_meter_provider(meter_provider)
    runtime = TelemetryRuntime(tracer_provider, meter_provider)
    runtime.start_heartbeat()
    return runtime


def validate_endpoint(value: str) -> None:
    parsed = urlparse(value)
    invalid = any(
        (
            parsed.scheme not in {"http", "https"},
            not parsed.hostname,
            bool(parsed.username),
            bool(parsed.password),
            bool(parsed.query),
            bool(parsed.fragment),
            any(ord(character) < 32 for character in value),
        )
    )
    if invalid:
        raise ValueError("OpenTelemetry OTLP endpoint is invalid")
    try:
        parsed.port
    except ValueError as error:
        raise ValueError("OpenTelemetry OTLP endpoint is invalid") from error


def signal_endpoint(value: str, signal: str) -> str:
    parsed = urlparse(value)
    if parsed.path.rstrip("/").endswith(f"/v1/{signal}"):
        return value
    return value.rstrip("/") + f"/v1/{signal}"


def sample_ratio() -> float:
    try:
        value = float(os.getenv("OCTO_OTEL_SAMPLE_RATIO", "1"))
    except ValueError:
        return 1.0
    return value if 0.0 <= value <= 1.0 else 1.0


def export_timeout() -> float:
    try:
        raw = os.getenv("OTEL_EXPORTER_OTLP_TIMEOUT", "5").strip().lower()
        value = float(raw[:-1] if raw.endswith("s") else raw)
    except ValueError:
        return 5.0
    return value if 0.1 <= value <= 60.0 else 5.0

"""OpenTelemetry traces for the extractor.

Enabled when OTEL_EXPORTER_OTLP_ENDPOINT is set (the standard variable; the compose stack points it
at the collector's HTTP port). Without it, spans are created against a no-op provider, so the code
path is identical in tests, on a laptop and in production.
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

from opentelemetry import trace
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, SpanExporter

_provider: TracerProvider | None = None


def configure_tracing(
    exporter: SpanExporter | None = None, service_name: str | None = None
) -> TracerProvider | None:
    """Install a tracer provider once. With no exporter and no endpoint configured, do nothing."""
    global _provider  # noqa: PLW0603  (one provider per process, installed once)
    if _provider is not None:
        return _provider
    endpoint = os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT", "").strip()
    if exporter is None:
        if not endpoint:
            return None
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter  # noqa: PLC0415

        exporter = OTLPSpanExporter(endpoint=endpoint.rstrip("/") + "/v1/traces", timeout=10)
    name = service_name or os.environ.get("OTEL_SERVICE_NAME", "ghcn-elt")
    _provider = TracerProvider(resource=Resource.create({SERVICE_NAME: name}))
    _provider.add_span_processor(BatchSpanProcessor(exporter))
    trace.set_tracer_provider(_provider)
    return _provider


def flush_tracing(timeout_ms: int = 5000) -> None:
    if _provider is not None:
        _provider.force_flush(timeout_millis=timeout_ms)


@contextmanager
def span(name: str, **attributes: Any) -> Iterator[trace.Span]:
    """A span on the ghcn_elt tracer; attributes may be added by the caller while it is open."""
    # Prefer the provider this module installed: the global one can only be set once per process.
    tracer = (_provider or trace.get_tracer_provider()).get_tracer("ghcn_elt")
    with tracer.start_as_current_span(name) as s:
        for k, v in attributes.items():
            s.set_attribute(k, v)
        yield s

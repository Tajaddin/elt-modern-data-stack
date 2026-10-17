from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from ghcn_elt import tracing


def test_spans_carry_attributes_when_a_provider_is_installed(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setattr(tracing, "_provider", None)
    exporter = InMemorySpanExporter()
    provider = tracing.configure_tracing(exporter=exporter, service_name="ghcn-test")
    assert provider is not None
    assert tracing.configure_tracing(exporter=exporter) is provider  # configured once

    with tracing.span("ghcn.extract", rows=42) as s:
        s.set_attribute("bytes", 7)
    tracing.flush_tracing()

    spans = exporter.get_finished_spans()
    assert [sp.name for sp in spans] == ["ghcn.extract"]
    assert spans[0].attributes["rows"] == 42
    assert spans[0].attributes["bytes"] == 7
    assert spans[0].resource.attributes["service.name"] == "ghcn-test"


def test_no_endpoint_means_no_provider(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setattr(tracing, "_provider", None)
    monkeypatch.delenv("OTEL_EXPORTER_OTLP_ENDPOINT", raising=False)
    assert tracing.configure_tracing() is None
    # Spans are still usable (no-op) so callers never need to check.
    with tracing.span("ghcn.noop", rows=1):
        pass
    tracing.flush_tracing()

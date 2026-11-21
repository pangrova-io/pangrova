---
title: "Observability Configuration"
description: "OpenTelemetry configuration keys"
---

# Observability

Coordinator and Worker accept the same OpenTelemetry settings in their respective YAML files. Configure both the service name and the OTLP endpoint to export telemetry. With neither exporter nor sampling settings configured, both roles use the local NoOp providers. Incomplete exporter settings fail at startup when observability is enabled.


## Observability Configuration (`observability`)

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `observability.enabled` | bool | `true` | Enable the observability module (feature flag, shared by coordinator and worker) |
| `observability.service-name` | string | — | OTel service name for this process (e.g. `"pangrova-worker"`) |
| `observability.otlp-endpoint` | string | — | OTel OTLP exporter endpoint URL (e.g. `http://otel-collector:4317`) |
| `observability.sampling.probability` | number | 0.01 in production, 1.0 otherwise | Fraction of traces recorded with probability sampling |
| `observability.sampling.strategy` | string | `PROBABILITY` | `PROBABILITY` or `RATE_LIMITING` |
| `observability.sampling.traces-per-second` | number | 1 | Positive token refill rate for rate-limited root traces |
| `observability.otlp-headers` | string | — | Comma-separated HTTP header name=value pairs |
| `observability.otlp-compression` | string | `none` | `none` or `gzip` |
| `observability.otlp-tls-cert-path` | path | — | Certificates trusted by the OTLP client |
| `observability.metrics.interval-ms` | milliseconds | 60000 | Interval between metric exports |

**Worker configuration**

```yaml
observability:
  enabled: true
  service-name: "pangrova-worker"
  otlp-endpoint: "http://otel-collector:4317"
```

---

## Notes

- The resilience module also exports circuit-breaker metrics
  (`resilience.circuitbreaker.state`, `resilience.circuitbreaker.failure_rate`,
  `resilience.circuitbreaker.buffered_calls`, ...) when observability is
  enabled.
- Coordinator-side structured logs use `event=`/`message=` key-value pairs and
  are emitted via SLF4J.

**See Also:** [Configuration model](index.md), [Worker](worker.md)

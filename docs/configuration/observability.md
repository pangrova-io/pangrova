---
title: "Observability"
description: "OpenTelemetry configuration keys"
---

# Observability

Observability configuration keys. These keys apply to `worker.yml`; the
worker bootstrap wires the OpenTelemetry exporter from them.


## Observability Configuration (`observability`)

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `observability.enabled` | bool | `true` | Enable the observability module (feature flag, shared by coordinator and worker) |
| `observability.service-name` | string | — | OTel service name for this worker (e.g. `"pangrova-worker"`) |
| `observability.otlp-endpoint` | string | — | OTel OTLP exporter endpoint URL (e.g. `http://otel-collector:4317`) |

**Example:**

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

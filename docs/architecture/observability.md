---
title: "Observability"
sidebar_label: "Observability"
---

# Pangrova — Observability

> **Accuracy scope.** This is a living architecture document. Content explicitly marked
> draft, proposed, planned, phase, target, or remaining work is design intent rather
> than implemented support. For current behavior, code wiring and end-to-end tests
> take precedence.

> **Summary.** The OpenTelemetry + Prometheus observability model — distributed tracing, metrics, and structured logs.

## 1. Overall Architecture

Coordinator and Worker bootstraps own their `TelemetryInitializer` and inject
its configured `Meter` and `Tracer` into `OtelMetricsProvider` and
`OtelTracingProvider`. These two implementations are not class-path
`ServiceLoader` providers: they require the configured SDK and its lifecycle.
Registering them in `META-INF/services` fails the
[JDK provider-constructor contract](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/util/ServiceLoader.html#developing-service-providers).
Logging and task-context wrapping retain their independently constructible
service registrations.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        Observability Architecture                            │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │                         Application Layer                              │  │
│  │  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐       │  │
│  │  │   Coordinator   │  │     Worker      │  │    Connector    │       │  │
│  │  │ @Traced @Metered│  │ @Traced @Metered│  │ @Traced @Metered│       │  │
│  │  └────────┬────────┘  └────────┬────────┘  └────────┬────────┘       │  │
│  └───────────┼────────────────────┼────────────────────┼─────────────────┘  │
│              │                    │                    │                    │
│  ┌───────────▼────────────────────▼────────────────────▼─────────────────┐  │
│  │                    Observability SPI Layer                             │  │
│  │  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐       │  │
│  │  │ TracingProvider │  │ MetricsProvider │  │ LoggingProvider │       │  │
│  │  └────────┬────────┘  └────────┬────────┘  └────────┬────────┘       │  │
│  └───────────┼────────────────────┼────────────────────┼─────────────────┘  │
│              │                    │                    │                    │
│  ┌───────────▼────────────────────▼────────────────────▼─────────────────┐  │
│  │                    OpenTelemetry SDK                                   │  │
│  │  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐       │  │
│  │  │  Trace Exporter │  │ Metric Exporter │  │   Log Exporter  │       │  │
│  │  │  (OTLP/Jaeger)  │  │ (OTLP/Prom)     │  │  (OTLP/Loki)    │       │  │
│  │  └────────┬────────┘  └────────┬────────┘  └────────┬────────┘       │  │
│  └───────────┼────────────────────┼────────────────────┼─────────────────┘  │
│              ▼                    ▼                    ▼                    │
│  ┌───────────────────┐  ┌───────────────────┐  ┌───────────────────┐      │
│  │      Jaeger       │  │    Prometheus     │  │       Loki        │      │
│  │ (Distributed      │  │ (Metrics Storage) │  │ (Log Aggregation) │      │
│  │  Tracing)         │  │                   │  │                   │      │
│  └─────────┬─────────┘  └─────────┬─────────┘  └─────────┬─────────┘      │
│            └──────────────────────┼──────────────────────┘                  │
│                                   ▼                                         │
│                        ┌───────────────────┐                               │
│                        │      Grafana      │                               │
│                        │   (Visualization) │                               │
│                        └───────────────────┘                               │
└─────────────────────────────────────────────────────────────────────────────┘
```

## 2. OpenTelemetry Integration

### 2.1 SPI Definitions

```java
// TracingProvider SPI
public interface TracingProvider {
    Span startSpan(String name, SpanKind kind);
    Span startSpan(String name, SpanKind kind, Context parent);
    Span currentSpan();
    <C> void inject(Context context, C carrier, TextMapSetter<C> setter);
    <C> Context extract(Context context, C carrier, TextMapGetter<C> getter);
}

// MetricsProvider SPI
public interface MetricsProvider {
    LongCounter createCounter(String name, String description, String unit);
    ObservableDoubleGauge createGauge(String name, String description,
                                       String unit, Supplier<Double> supplier);
    DoubleHistogram createHistogram(String name, String description,
                                     String unit, List<Double> boundaries);
    LongUpDownCounter createUpDownCounter(String name, String description, String unit);
}

// LoggingProvider SPI
//
public interface LoggingProvider {
    Logger getLogger(String name);
    Logger getLogger(String name, Context context);
}
```

### 2.2 OpenTelemetry Configuration

```java
public class OpenTelemetryConfig {
    private String serviceName = "pangrova";
    private String serviceVersion = "1.0.0";
    private String otlpEndpoint = "http://localhost:4317";
    private boolean tracingEnabled = true;
    private boolean metricsEnabled = true;
    private boolean loggingEnabled = true;
    private double tracingSampleRate = 1.0;
    private Duration metricsInterval = Duration.ofSeconds(60);
}
```

## 3. Core Metric Definitions

### 3.1 Coordinator Metrics

| Metric Name | Type | Description |
|--------|------|------|
| `pangrova.coordinator.queries.submitted` | Counter | Total queries submitted |
| `pangrova.coordinator.queries.completed` | Counter | Total queries completed successfully |
| `pangrova.coordinator.queries.failed` | Counter | Total queries failed |
| `pangrova.coordinator.queries.running` | UpDownCounter | Currently running queries |
| `pangrova.coordinator.query.latency` | Histogram | Query execution latency |
| `pangrova.coordinator.tasks.scheduled` | Counter | Total tasks scheduled |
| `pangrova.coordinator.scheduling.latency` | Histogram | Task scheduling latency |
| `pangrova.coordinator.workers.active` | UpDownCounter | Active Worker count |

### 3.2 Worker Metrics

| Metric Name | Type | Description |
|--------|------|------|
| `pangrova.worker.tasks.received` | Counter | Total tasks received |
| `pangrova.worker.tasks.completed` | Counter | Total tasks completed |
| `pangrova.worker.tasks.failed` | Counter | Total tasks failed |
| `pangrova.worker.tasks.running` | UpDownCounter | Currently running tasks |
| `pangrova.worker.task.latency` | Histogram | Task execution latency |
| `pangrova.worker.memory.used` | Gauge | Current memory usage |
| `pangrova.worker.memory.reserved` | Gauge | Reserved memory |
| `pangrova.worker.io.bytes.read` | Counter | Total bytes read |
| `pangrova.worker.io.bytes.written` | Counter | Total bytes written |
| `pangrova.worker.io.rows.read` | Counter | Total rows read |
| `pangrova.worker.io.rows.written` | Counter | Total rows written |

## 4. Distributed Tracing

### 4.1 Query Tracing

```java
public class QueryTracer {
    public QueryExecution traceQuery(String queryId, String sql,
                                      Supplier<QueryExecution> execution) {
        Span span = tracingProvider.startSpan("query.execute", SpanKind.SERVER);
        try (Scope scope = span.makeCurrent()) {
            span.setAttribute("query.id", queryId);
            span.setAttribute("query.sql", truncateSql(sql));

            // Parse phase
            tracePhase("query.parse", () -> { /* parse logic */ });
            // Optimize phase
            tracePhase("query.optimize", () -> { /* optimize logic */ });
            // Schedule phase
            tracePhase("query.schedule", () -> { /* schedule logic */ });

            return execution.get();
        } catch (Exception e) {
            span.setStatus(StatusCode.ERROR, e.getMessage());
            span.recordException(e);
            throw e;
        } finally {
            span.end();
        }
    }
}
```

### 4.2 Cross-Service Tracing



The Control Plane REST layer propagates the W3C Trace Context (`traceparent` + `tracestate`) via an HTTP Filter, making it easy to connect REST calls with traces in the Data Plane (Arrow Flight/gRPC).

HTTP filter configuration example:

```yaml
http:
  filters:
    - name: trace-propagation
      config:
        header: traceparent
        vendor: w3c
```

`TracingHttpFilter` implementation:

```java
public class TracingHttpFilter implements Filter {
    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {
        HttpServletRequest http = (HttpServletRequest) request;

        // Extract Context from HTTP headers
        Context extractedContext = tracingProvider.extract(
            Context.current(), http, HttpHeaderGetter.INSTANCE);

        Span span = tracingProvider.startSpan(
            http.getMethod() + " " + http.getRequestURI(),
            SpanKind.SERVER, extractedContext);

        try (Scope scope = span.makeCurrent()) {
            chain.doFilter(request, response);
        } catch (Exception e) {
            span.setStatus(StatusCode.ERROR, e.getMessage());
            span.recordException(e);
            throw e;
        } finally {
            span.end();
        }
    }
}
```

Note: The data plane Arrow Flight is still based on gRPC streaming; a corresponding gRPC interceptor must be configured on the Flight Server side for Trace propagation.

### 4.3 Cross-Layer Trace Correlation (Java ↔ Native)


```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                      Cross-Layer Trace Correlation                               │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  Problem: Java/Native layer Spans are loosely correlated, making it hard to trace the full execution path │
│  Solution: unified TraceId propagation; the Native layer integrates the OpenTelemetry C++ SDK   │
│                                                                                  │
│  Trace propagation flow:                                                         │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  Java Layer                                                              │   │
│  │  ┌─────────────────────────────────────────────────────────────────┐   │   │
│  │  │  Span: query.execute (trace_id=abc123)                          │   │   │
│  │  │    └─ Span: task.submit                                         │   │   │
│  │  │         └─ Span: native.execute ──────────────────────────┐    │   │   │
│  │  └──────────────────────────────────────────────────────────┼────┘   │   │
│  └─────────────────────────────────────────────────────────────┼─────────┘   │
│                                                                 │             │
│                    JNI Call (propagate trace_id, span_id)      │             │
│                                                                 ▼             │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  Native Layer (C++ OpenTelemetry SDK)                                    │   │
│  │  ┌─────────────────────────────────────────────────────────────────┐   │   │
│  │  │  Span: velox.task.execute (parent=native.execute)               │   │   │
│  │  │    ├─ Span: velox.pipeline.run                                  │   │   │
│  │  │    │    ├─ Span: velox.operator.tablescan                       │   │   │
│  │  │    │    ├─ Span: velox.operator.filter                          │   │   │
│  │  │    │    └─ Span: velox.operator.aggregate                       │   │   │
│  │  │    └─ Span: velox.io.read                                       │   │   │
│  │  └─────────────────────────────────────────────────────────────────┘   │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

```cpp
// NativeTracer.h - Native-layer Trace integration
#pragma once

#include <opentelemetry/trace/provider.h>
#include <opentelemetry/context/propagation/text_map_propagator.h>

namespace pangrova {
namespace observability {

/**
 * Native-layer Tracer
 * Receives the TraceContext propagated from the Java layer and creates child Spans
 */
class NativeTracer {
public:
    static NativeTracer& instance();

    /**
     * Create a Span from the context propagated by the Java layer
     * @param traceId    Java-layer trace_id (hex string)
     * @param spanId     Java-layer parent span_id
     * @param spanName   Span name
     */
    opentelemetry::nostd::shared_ptr<opentelemetry::trace::Span>
    startSpanWithParent(const std::string& traceId,
                        const std::string& spanId,
                        const std::string& spanName);

    /** Create child span under current context */
    opentelemetry::nostd::shared_ptr<opentelemetry::trace::Span>
    startSpan(const std::string& spanName);

private:
    opentelemetry::nostd::shared_ptr<opentelemetry::trace::Tracer> tracer_;
};

/** RAII Span guard */
class SpanScope {
public:
    explicit SpanScope(opentelemetry::nostd::shared_ptr<opentelemetry::trace::Span> span);
    ~SpanScope();
private:
    opentelemetry::nostd::shared_ptr<opentelemetry::trace::Span> span_;
    opentelemetry::context::Token token_;
};

} // namespace observability
} // namespace pangrova
```

**Full NativeTracer implementation** (`NativeTracer.cpp`) — OpenTelemetry C++ SDK integration:

```cpp
// NativeTracer.cpp - Complete OpenTelemetry C++ SDK integration
#include "pangrova/observability/NativeTracer.h"

#include <opentelemetry/sdk/trace/tracer_provider.h>
#include <opentelemetry/sdk/trace/simple_processor.h>
#include <opentelemetry/sdk/trace/batch_span_processor.h>
#include <opentelemetry/exporters/otlp/otlp_grpc_exporter.h>
#include <opentelemetry/sdk/resource/resource.h>
#include <opentelemetry/trace/context.h>
#include <opentelemetry/trace/span_id.h>
#include <opentelemetry/trace/trace_id.h>

namespace pangrova {
namespace observability {

namespace trace_api = opentelemetry::trace;
namespace trace_sdk = opentelemetry::sdk::trace;
namespace otlp      = opentelemetry::exporter::otlp;
namespace resource  = opentelemetry::sdk::resource;

/**
 * Initialize OpenTelemetry C++ SDK with OTLP gRPC exporter.
 * Called once during Worker native library loading (JNI_OnLoad).
 *
 * @param otlpEndpoint  OTLP collector endpoint (e.g. "localhost:4317")
 * @param serviceName   Service name for resource attribution (e.g. "pangrova-native")
 */
void NativeTracer::initialize(const std::string& otlpEndpoint,
                               const std::string& serviceName) {
    // 1. Create OTLP gRPC exporter
    otlp::OtlpGrpcExporterOptions exporterOpts;
    exporterOpts.endpoint = otlpEndpoint;
    auto exporter = std::make_unique<otlp::OtlpGrpcExporter>(exporterOpts);

    // 2. Create BatchSpanProcessor (async, non-blocking)
    trace_sdk::BatchSpanProcessorOptions processorOpts;
    processorOpts.max_queue_size = 2048;
    processorOpts.schedule_delay_millis = std::chrono::milliseconds(5000);
    processorOpts.max_export_batch_size = 512;
    auto processor = std::make_unique<trace_sdk::BatchSpanProcessor>(
        std::move(exporter), processorOpts);

    // 3. Create Resource with service name
    auto res = resource::Resource::Create({
        {"service.name", serviceName},
        {"service.version", "1.0.0"},
        {"telemetry.sdk.language", "cpp"}
    });

    // 4. Create TracerProvider and register globally
    auto provider = std::make_shared<trace_sdk::TracerProvider>(
        std::move(processor), res);
    trace_api::Provider::SetTracerProvider(provider);

    // 5. Get tracer instance
    tracer_ = provider->GetTracer("pangrova-native", "1.0.0");
}

NativeTracer& NativeTracer::instance() {
    static NativeTracer inst;
    return inst;
}

/**
 * Create a child span from Java-provided W3C trace context.
 * Reconstructs the parent SpanContext from traceId + spanId hex strings.
 */
opentelemetry::nostd::shared_ptr<trace_api::Span>
NativeTracer::startSpanWithParent(const std::string& traceId,
                                   const std::string& spanId,
                                   const std::string& spanName) {
    // Reconstruct parent SpanContext from hex strings
    trace_api::TraceId tid;
    trace_api::SpanId sid;

    // Parse 32-char hex → 16-byte TraceId
    uint8_t traceIdBytes[16];
    for (int i = 0; i < 16; ++i) {
        traceIdBytes[i] = static_cast<uint8_t>(
            std::stoul(traceId.substr(i * 2, 2), nullptr, 16));
    }
    tid = trace_api::TraceId(traceIdBytes);

    // Parse 16-char hex → 8-byte SpanId
    uint8_t spanIdBytes[8];
    for (int i = 0; i < 8; ++i) {
        spanIdBytes[i] = static_cast<uint8_t>(
            std::stoul(spanId.substr(i * 2, 2), nullptr, 16));
    }
    sid = trace_api::SpanId(spanIdBytes);

    // Create remote parent SpanContext
    auto parentCtx = trace_api::SpanContext(
        tid, sid, trace_api::TraceFlags(trace_api::TraceFlags::kIsSampled),
        /*is_remote=*/true);

    // Start child span with parent context
    trace_api::StartSpanOptions opts;
    opts.parent = parentCtx;

    return tracer_->StartSpan(spanName, opts);
}

opentelemetry::nostd::shared_ptr<trace_api::Span>
NativeTracer::startSpan(const std::string& spanName) {
    return tracer_->StartSpan(spanName);
}

// --- SpanScope RAII implementation ---

SpanScope::SpanScope(opentelemetry::nostd::shared_ptr<trace_api::Span> span)
    : span_(std::move(span))
    , token_(opentelemetry::context::RuntimeContext::Attach(
          opentelemetry::trace::SetSpan(
              opentelemetry::context::RuntimeContext::GetCurrent(), span_))) {}

SpanScope::~SpanScope() {
    span_->End();
}

} // namespace observability
} // namespace pangrova
```

**JNI_OnLoad initialization entry point** — initialize OpenTelemetry when the Native library is loaded:

```cpp
// pangrova_jni_init.cpp
#include <jni.h>
#include "pangrova/observability/NativeTracer.h"

extern "C" JNIEXPORT jint JNICALL JNI_OnLoad(JavaVM* vm, void* /*reserved*/) {
    // Read OTLP endpoint from system property (set by Java layer)
    // Default: localhost:4317
    std::string otlpEndpoint = getSystemProperty(vm, "pangrova.native.otlp.endpoint",
                                                  "localhost:4317");
    std::string serviceName = getSystemProperty(vm, "pangrova.native.service.name",
                                                 "pangrova-native");

    pangrova::observability::NativeTracer::instance().initialize(otlpEndpoint, serviceName);

    return JNI_VERSION_1_8;
}

extern "C" JNIEXPORT void JNICALL JNI_OnUnload(JavaVM* /*vm*/, void* /*reserved*/) {
    // Flush pending spans before unload
    auto provider = opentelemetry::trace::Provider::GetTracerProvider();
    if (auto sdkProvider =
            std::dynamic_pointer_cast<opentelemetry::sdk::trace::TracerProvider>(provider)) {
        sdkProvider->ForceFlush(std::chrono::milliseconds(5000));
    }
}
```

**Operator-level Span usage example** — create a child Span inside a Velox Pipeline:

```cpp
// Example: TableScan operator with tracing
void TableScanOperator::getOutput(RowVectorPtr& output) {
    auto& tracer = pangrova::observability::NativeTracer::instance();
    auto span = tracer.startSpan("velox.operator.tablescan");
    pangrova::observability::SpanScope scope(span);

    span->SetAttribute("operator.id", operatorId_);
    span->SetAttribute("io.file_format", "parquet");

    // Actual scan logic
    auto batch = reader_->readNext();

    span->SetAttribute("operator.rows_out", batch ? batch->size() : 0);
    span->SetAttribute("io.bytes_read", reader_->bytesRead());

    output = std::move(batch);
}
```

```java
// NativeTraceBridge.java - Java-side Trace Context propagation
public class NativeTraceBridge {

    /**
     * Submit a Native task, propagating the Trace Context (Pull mode: no callback parameter)
     */
    public long submitWithTrace(long executorHandle) {
        Span currentSpan = Span.current();
        SpanContext ctx = currentSpan.getSpanContext();

        // Submit task with trace context, returns task handle
        return nativeSubmitWithTrace(
            executorHandle,
            ctx.getTraceId(),      // 32 char hex
            ctx.getSpanId(),       // 16 char hex
            ctx.getTraceFlags().asByte()
        );
    }

    private static native long nativeSubmitWithTrace(
        long executorHandle,
        String traceId,
        String parentSpanId,
        byte traceFlags);
}
```

### 4.4 Native-Layer Profiling

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         Native Layer Profiling                                   │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  Problem: performance issues in the C++ layer are hard to pinpoint             │
│  Solution: integrate perf/async-profiler and expose Native-layer flame graphs   │
│                                                                                  │
│  Profiling modes:                                                                │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  1. On-Demand Profiling: triggered via HTTP API, collects a specified window │   │
│  │  2. Continuous Profiling: low-overhead continuous collection, reported to Pyroscope/Parca │   │
│  │  3. Query-Level Profiling: enabled for a specific query, for problem diagnosis │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
│  Collected content:                                                              │
│  • CPU Profile: call stacks, hot functions                                       │
│  • Memory Profile: allocation hotspots, leak detection                           │
│  • Lock Contention: lock contention analysis                                     │
│  • I/O Profile: file read/write latency distribution                            │
│                                                                                  │
│  Output formats:                                                                 │
│  • Flame Graph (SVG/HTML)                                                       │
│  • pprof format (compatible with the Go pprof toolchain)                         │
│  • Trace Event format (Chrome Tracing)                                           │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

```cpp
// NativeProfiler.h
#pragma once

namespace pangrova {
namespace profiling {

enum class ProfileType { CPU, MEMORY, LOCK, IO };

/**
 * Native-layer Profiler
 */
class NativeProfiler {
public:
    static NativeProfiler& instance();

    /** Start profiling for specified duration */
    void startProfiling(ProfileType type, std::chrono::seconds duration);

    /** Stop and get profile data */
    std::string stopAndGetProfile(ProfileType type, const std::string& format);

    /** Enable continuous profiling (low overhead) */
    void enableContinuousProfiling(const std::string& pyroscopeEndpoint);

    /** Profile specific query */
    void startQueryProfiling(const std::string& queryId);
    std::string stopQueryProfiling(const std::string& queryId);

private:
    // Integration with perf/async-profiler
};

} // namespace profiling
} // namespace pangrova
```

```java
// ProfilerEndpoint.java - HTTP API for profiling
@RestController
@RequestMapping("/api/v1/profiler")
public class ProfilerEndpoint {

    @PostMapping("/native/start")
    public void startNativeProfiling(
            @RequestParam ProfileType type,
            @RequestParam(defaultValue = "30") int durationSeconds) {
        nativeProfiler.start(type, Duration.ofSeconds(durationSeconds));
    }

    @GetMapping("/native/flamegraph")
    public ResponseEntity<byte[]> getNativeFlameGraph(@RequestParam ProfileType type) {
        byte[] svg = nativeProfiler.getFlameGraph(type);
        return ResponseEntity.ok()
            .contentType(MediaType.valueOf("image/svg+xml"))
            .body(svg);
    }

    @GetMapping("/query/{queryId}/profile")
    public QueryProfile getQueryProfile(@PathVariable String queryId) {
        // Return combined Java + Native profile for specific query
        return QueryProfile.builder()
            .javaProfile(javaProfiler.getProfile(queryId))
            .nativeProfile(nativeProfiler.getProfile(queryId))
            .build();
    }
}
```

### 4.5 Query Resource Audit

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         Query Resource Audit                                     │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  Problem: historical query resource consumption cannot be traced               │
│  Solution: persist a resource-usage report after query completion              │
│                                                                                  │
│  Audit content:                                                                  │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  QueryAuditRecord {                                                      │   │
│  │    queryId, user, source, sql,                                          │   │
│  │    startTime, endTime, state,                                           │   │
│  │                                                                          │   │
│  │    // Resource usage                                                     │   │
│  │    cpuTimeMs, wallTimeMs,                                               │   │
│  │    peakMemoryBytes, spilledBytes,                                       │   │
│  │    inputRows, outputRows,                                               │   │
│  │    inputBytes, outputBytes,                                             │   │
│  │    shuffleReadBytes, shuffleWriteBytes,                                 │   │
│  │                                                                          │   │
│  │    // Cost breakdown by stage                                           │   │
│  │    stages: [{stageId, cpuTimeMs, memoryBytes, ioBytes}],                │   │
│  │                                                                          │   │
│  │    // Worker distribution                                               │   │
│  │    workers: [{workerId, taskCount, cpuTimeMs}]                          │   │
│  │  }                                                                       │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
│  Current storage: structured log plus bounded process-local profile cache              │
│  Purpose: cost allocation, capacity planning, anomalous-query detection, SLA reporting │


│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### Resource audit admission and shutdown

Query audit values and the admission sink live in `pangrova-spi` under
`org.pangrova.spi.query.audit`; the storage port lives in `pangrova-persistence-api`
under `org.pangrova.persistence.api.query.audit`. The runtime service remains
`org.pangrova.query.audit.QueryAuditService`. Each Coordinator composes its own
`QueryAuditServices`: admission and profile lookup
share one `org.pangrova.persistence.memory.query.audit.InMemoryQueryAuditStore` instance.
Independent Coordinators do not share a global cache.
API consumers do not load the service, cache or JDBC backend.

Terminal query records preserve the query-owned submitting principal and resolved
tenant for both success and failure. Anonymous rejection is explicitly attributed
to `anonymous`, without inventing a tenant. The record persistence branch receives
the same immutable attribution as the security event branch; it must not drop
identity when the event backend is absent. This does not yet close the resource
audit integration gap: terminal resource records currently lack actual stage/task
and Native usage aggregation, and zero placeholders are not measured usage.

The unused asynchronous `JdbcAuditStore` implementation has been removed: no production
constructor, factory or service registration selected it. Its private queue tests were
removed with it; the active audit-service and shared-cache tests remain. The historical
`query_audit_records` migration is preserved byte-for-byte, but this table is not the
current resource-audit persistence path. Durable completion/split events use
`org.pangrova.persistence.jdbc.query.event.JdbcQueryEventListener` and the separate
`query_events` / `query_split_profiles` tables. These facts must not be presented as
a durable resource-usage audit ledger.

Metric getters intentionally expose the same registered live instrument, JVM collectors
retain live management beans, and no-op instruments are stateless shared singletons.
Immutable profile and alert-rule collections are defensively copied; native metrics
arrays are copied on capture and access. Static-analysis exceptions for these shared
handles and immutable lists match exact classes, methods, fields, and bug patterns only.

## 5. Native-Layer Metrics

### 5.1 Native Metric Definitions

| Metric Name | Type | Description |
|--------|------|------|



| `pangrova.native.memory.allocated` | Gauge | Native allocated memory (source: Java MemoryManager, not Native Pull — 03 §5.3 TaskStats has `peak_memory_bytes` only; no `allocated_memory_bytes` field) |
| `pangrova.native.memory.peak` | Gauge | Native peak memory (unit: By) |
| `pangrova.native.spill.bytes` | Counter | Bytes spilled to disk (unit: By) |
| `pangrova.native.velox.cpu.time` | Counter | Velox CPU time (unit: ns) |
| `pangrova.native.velox.wall.time` | Counter | Velox wall-clock time (unit: ns) |
| `pangrova.native.io.read.bytes` | Counter | Native I/O bytes read (unit: By) |
| `pangrova.native.io.output.bytes` | Counter | Native I/O bytes written (unit: By) |
| `pangrova.native.io.read.latency` | Histogram | Native I/O read latency (unit: ns) |
| `pangrova.native.io.row.groups.total` | Counter | Total row groups encountered |
| `pangrova.native.io.row.groups.skipped` | Counter | Row groups skipped by predicate pushdown |
| `pangrova.native.io.bloom.filter.checks` | Counter | Bloom filter checks performed |
| `pangrova.native.io.bloom.filter.skipped` | Counter | Rows skipped by bloom filter |
| `pangrova.native.io.delete.rows.filtered` | Counter | Delete rows filtered during merge-on-read |
| `pangrova.native.operator.rows.in` | Counter | Operator input rows (by operator type) |
| `pangrova.native.operator.rows.out` | Counter | Operator output rows (by operator type) |

### 5.2 Native Metrics Reporting

```cpp
// NativeMetrics.h
namespace pangrova {
namespace metrics {

/**
 * Native-layer metrics collector
 * Metrics are accumulated on the Native side; Java pulls them via nativeGetTaskStats()
 */
class NativeMetricsCollector {
public:
    static NativeMetricsCollector& instance();

    void recordMemoryUsage(int64_t allocated, int64_t peak);
    void recordSpill(int64_t bytes);
    void recordOperatorStats(const std::string& operatorType,
                             int64_t rowsIn, int64_t rowsOut, int64_t cpuNanos);
    void recordIOStats(int64_t readBytes, int64_t readLatencyNanos);

    /** Snapshot current metrics (called by Java via nativeGetTaskStats) */
    TaskStats snapshot() const;

private:
    // Accumulate metrics in-process, no JNI callback
    std::atomic<int64_t> totalMemoryAllocated_{0};
    std::atomic<int64_t> peakMemory_{0};
    std::atomic<int64_t> spillBytes_{0};
    std::mutex mutex_;
    std::vector<OperatorStats> operatorStats_;
};

} // namespace metrics
} // namespace pangrova
```

### 5.3 Native Metrics Pull Frequency Contract

The Java layer actively pulls Native metrics via `nativeGetTaskStats()` (see [native-execution.md §5.1](./native-execution.md) NativeRuntime). This section defines the pull-frequency contract and backpressure behavior.

**Pull Frequency Contract**:

| Parameter | Default | Range | Description |
|-----------|---------|-------|-------------|
| `native.metrics.pull-interval` | 1s | 100ms – 30s | Interval at which Java pulls Native TaskStats |
| `native.metrics.pull-timeout` | 500ms | 50ms – 5s | Timeout for a single `nativeGetTaskStats()` call |
| `native.metrics.stale-threshold` | 3× pull-interval | — | If no successful pull within this time, mark metrics as stale |

**Pull Loop Design**:

```java
/**
 * Native metrics pull loop (Worker side)
 *
 * Each TaskExecutor has one pull loop, driven by a Java ScheduledExecutor.
 * A pull failure does not affect Task execution; it only makes metrics temporarily invisible.
 */
public class NativeMetricsPuller {

    private final long executorHandle;
    private final Duration pullInterval;
    private final Duration pullTimeout;
    private final Duration staleThreshold;
    private volatile Instant lastSuccessfulPull = Instant.now();
    private final AtomicInteger consecutiveFailures = new AtomicInteger(0);

    /**
     * Scheduled pull — called by ScheduledExecutorService
     */
    public void pull() {
        try {
            byte[] statsBytes = NativeRuntime.nativeGetTaskStats(executorHandle);
            TaskStats stats = TaskStats.parseFrom(statsBytes);

            metricsRegistry.updateNativeMetrics(executorHandle, stats);
            lastSuccessfulPull = Instant.now();
            consecutiveFailures.set(0);

        } catch (Exception e) {
            int failures = consecutiveFailures.incrementAndGet();
            handlePullFailure(failures, e);
        }
    }

    /**
     * Pull failure handling — backpressure & staleness
     *
     * Failure does NOT cancel the task. Metrics become stale but execution continues.
     */
    private void handlePullFailure(int failures, Exception e) {
        Duration sinceLastSuccess = Duration.between(lastSuccessfulPull, Instant.now());

        if (sinceLastSuccess.compareTo(staleThreshold) > 0) {
            // Mark metrics as stale — Prometheus label: pangrova_native_metrics_stale=1
            metricsRegistry.markStale(executorHandle);
            log.warn("Native metrics stale for task {}, last success: {}",
                     executorHandle, lastSuccessfulPull);
        }

        // Adaptive backoff: slow down pull frequency on repeated failures
        // to avoid JNI call overhead when Native is under heavy load
        if (failures >= 3) {
            log.warn("Native metrics pull failed {} times, backing off", failures);
        }
    }
}
```

**Backpressure Behavior** (Native metrics accumulation rate > Java pull rate):

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                  Native Metrics Pull Backpressure                                │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  Scenario: Native emits metrics at high speed, Java pulling cannot keep up      │
│                                                                                  │
│  Design: no extra backpressure needed — the Native-side snapshot() is an instantaneous snapshot │
│                                                                                  │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  Native side:                                                            │   │
│  │  • NativeMetricsCollector uses atomic counters, lock-free writes          │   │
│  │  • snapshot() reads current values, O(1) time complexity                 │   │
│  │  • No "backlog" problem — every snapshot is the latest state             │   │
│  │                                                                          │   │
│  │  Java side:                                                              │   │
│  │  • Intermediate states within the pull interval are skipped (sampling semantics, not event semantics) │   │
│  │  • Lower pull frequency → coarser metric granularity, but final state is not lost │   │
│  │  • Pull timeout → skip this round, retry next round                      │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
│  Exception handling:                                                             │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  Scenario                    │ Behavior                                  │   │
│  │  ─────────────────────────────────────────────────────────────────────  │   │
│  │  nativeGetTaskStats timeout  │ Skip, retry next interval                │   │
│  │  Native task already finished│ Return final snapshot, puller stops      │   │
│  │  JNI call throws exception   │ Log + increment failure counter          │   │
│  │  Consecutive failures ≥ 5    │ Reduce pull frequency to 5× interval    │   │
│  │  Stale threshold exceeded    │ Expose stale label in Prometheus metric │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

**Proposed configuration** (not part of the verified
[configuration key reference](../configuration/reference.md)):

```yaml
observability:
  native-metrics:
    pull-interval: 1s
    pull-timeout: 500ms
    stale-threshold: 3s
    max-consecutive-failures-before-backoff: 3
    backoff-multiplier: 5    # pull interval × 5 on repeated failures
```

---

## 6. Module Structure

```
pangrova-observability/pangrova-observability-core/
├── src/main/java/org/pangrova/observability/
│   ├── ObservabilityModule.java         # Module entry point
│   ├── config/                          # Configuration
│   │   ├── ObservabilityConfig.java
│   │   └── OpenTelemetryConfig.java
│   ├── tracing/                         # Tracing
│   │   ├── TracingProvider.java
│   │   ├── OtelTracingProvider.java
│   │   ├── QueryTracer.java
│   │   └── interceptor/
│   ├── metrics/                         # Metrics
│   │   ├── MetricsProvider.java
│   │   ├── OtelMetricsProvider.java
│   │   ├── CoordinatorMetrics.java
│   │   └── WorkerMetrics.java
│   ├── logging/                         # Logging
│   │   ├── LoggingProvider.java
│   │   └── StructuredLogger.java
│   └── annotation/                      # Annotations
│       ├── Traced.java
│       ├── Metered.java
│       └── aspect/
```


---

## 7. Logging Conventions

### 7.1 Log Format

All logs use a JSON structured format, making them easy to collect and query with Loki/ELK.

```json
{
  "timestamp": "2025-01-15T10:30:00.123Z",
  "level": "INFO",
  "logger": "org.pangrova.worker.TaskExecutor",
  "message": "Task completed",
  "trace_id": "abc123def456",
  "span_id": "789ghi",
  "query_id": "query-001",
  "task_id": "task-001",
  "worker_id": "worker-1",
  "tenant_id": "tenant-a",
  "duration_ms": 1234,
  "extra": {}
}
```

### 7.2 Log Level Conventions

| Level | Use Case | Example |
|-------|---------|------|
| ERROR | Failures requiring human intervention | Native crash, OOM, data corruption |
| WARN | Automatically recoverable anomalies | Retry succeeded, Spill triggered, circuit breaker opened |
| INFO | Key business events | Query submitted/completed, Task scheduling, Catalog registration |
| DEBUG | Development debugging info | SQL parse results, execution plan, Split assignment |
| TRACE | Extremely fine-grained tracing | Rows per batch, memory allocation details |

### 7.3 Log Context Propagation

```java
/**
 * Log context — automatically attached to all log output
 * Implemented via MDC (Mapped Diagnostic Context)
 */
public class LogContext {

    public static void withQuery(String queryId, String traceId, Runnable action) {
        MDC.put("query_id", queryId);
        MDC.put("trace_id", traceId);
        try {
            action.run();
        } finally {
            MDC.remove("query_id");
            MDC.remove("trace_id");
        }
    }
}
```

### 7.4 Log Sampling Strategy

| Scenario | Sampling Rate | Note |
|------|--------|------|
| ERROR/WARN | 100% | Full collection |
| INFO (query lifecycle) | 100% | Query submitted/completed/failed |
| DEBUG (execution plan) | 10% | Sampled by query_id hash |
| TRACE (batch-level) | 1% | Only enabled in diagnostic mode |

---

## 8. Tracing Span Definitions

### 8.1 Span Naming Conventions

Format: `{component}.{operation}`, all lowercase, dot-separated.

### 8.2 Core Span Hierarchy

```
query.execute                              (Coordinator, SERVER)
├── query.parse                            (Coordinator, INTERNAL)
├── query.optimize                         (Coordinator, INTERNAL)
│   ├── query.optimize.rule.{ruleName}     (Coordinator, INTERNAL)
│   └── query.optimize.statistics          (Coordinator, INTERNAL)
├── query.schedule                         (Coordinator, INTERNAL)
│   ├── query.schedule.split               (Coordinator, INTERNAL)
│   └── query.schedule.assign              (Coordinator, INTERNAL)
├── task.submit                            (Coordinator → Worker, CLIENT)
│   └── task.execute                       (Worker, SERVER)
│       ├── native.execute                 (Worker → Native, INTERNAL)
│       │   ├── velox.pipeline.run         (Native, INTERNAL)
│       │   │   ├── velox.operator.tablescan   (Native, INTERNAL)
│       │   │   ├── velox.operator.filter      (Native, INTERNAL)
│       │   │   ├── velox.operator.project     (Native, INTERNAL)
│       │   │   ├── velox.operator.hashjoin    (Native, INTERNAL)
│       │   │   └── velox.operator.aggregate   (Native, INTERNAL)
│       │   └── velox.io.read              (Native, CLIENT)
│       ├── shuffle.write                  (Worker, INTERNAL)
│       └── shuffle.read                   (Worker, CLIENT)
└── query.result.stream                    (Output Worker → Client, SERVER)
```

### 8.3 Span Attributes Standard

| Attribute | Type | Applicable Span | Note |
|-----------|------|-----------|------|
| `query.id` | string | all | Query ID |
| `query.sql` | string | query.execute | SQL text (truncated to 1024 characters) |
| `task.id` | string | task.* | Task ID |
| `worker.id` | string | task.*, native.* | Worker node ID |
| `operator.type` | string | velox.operator.* | Operator type |
| `operator.rows_in` | long | velox.operator.* | Input rows |
| `operator.rows_out` | long | velox.operator.* | Output rows |
| `io.bytes_read` | long | velox.io.read | Bytes read |
| `io.file_format` | string | velox.io.read | File format (parquet/orc) |
| `memory.peak_bytes` | long | task.execute | Peak memory |
| `spill.bytes` | long | task.execute | Spilled bytes |



### 8.5 Audit and Lineage Integration

### Security Audit vs Resource Audit

Pangrova has two complementary audit subsystems:

| Aspect | Security Audit ([12 §6](./security.md)) | Resource Audit (§4.5) |
|--------|---------------------------------------------|----------------------|
| Scope | Who did what (authentication, authorization, DDL, data access) | How much was consumed (CPU, memory, I/O, shuffle) |
| Storage | `audit_logs` table | Structured log and bounded process-local cache |
| Key fields | principal, clientIp, sql, success, accessedObjects | cpuTimeMs, peakMemoryBytes, spilledBytes, inputRows |
| Owner | Security audit service / Persistence JDBC | Query audit service / Persistence Memory |

The active security events and resource records use distinct contracts. Query identifiers
allow correlation, but the process-local resource cache is not a durable SQL join source.

Query lifecycle security events retain the submitting principal in the query-owned
state machine before ownership acquisition. Completion and failure use that captured
principal and the resolved tenant (`properties.tenant_id`), rather than an emitter-side
identity cache or the thread's current request context. Anonymous rejection is
explicitly attributed to `anonymous`. Authentication tokens and arbitrary session
properties are not retained for terminal audit attribution.

This in-process attribution is not a durable audit guarantee. The current session
journal does not persist the principal for recovery, and early admission/ownership
rejections can precede terminal-event emission. Those paths and process-loss delivery
require separate recovery and durability verification.

The Coordinator module owner also closes and joins its metadata invalidation
poller before the metadata data source is released. Ownership is registered
before schema validation or scheduler startup, so a later bootstrap failure still
enters the same cleanup path. Canceling only the leader-gated cleanup timer is
insufficient: the poller owns a separate scheduled executor.

### OpenLineage Trace Correlation

Data lineage events emitted by [11 §3 LineageEventEmitter](./data-governance.md) should include `traceId` in the OpenLineage `RunEvent.run.facets` to enable lineage-to-trace correlation:

```json
{
  "run": {
    "facets": {
      "tracing": {
        "traceId": "abc123def456",
        "spanId": "789ghi"
      }
    }
  }
}
```

This allows querying Jaeger traces from lineage events and vice versa.

### Alerting

Operational alerting guidance is defined in the [day-2 operations guide](../operations/day-2.md).
Security components emit alerts through the observability pipeline defined in §3
metrics and §7 logging.



## 9. Cross-Module Observability Reference

This document (observability.md) is the authoritative source for the observability framework. The following architecture documents contain module-specific Observability Integration sections that reference this document:

| Document | Section | Coverage |
|----------|---------|----------|
| [distributed.md](./distributed.md) | §7 | Cluster-level metrics and tracing |
| [query-execution.md](./query-execution.md) | §16 | Query lifecycle metrics and tracing |
| [native-execution.md](./native-execution.md) | §18 | Native operator and JNI metrics |
| [connectors.md](./connectors.md) | Observability §  | Connector I/O metrics |
| [query-optimization.md](./query-optimization.md) | Observability § | Optimizer statistics metrics |
| [resource-management.md](./resource-management.md) | Observability § | Memory and resource pool metrics |
| [metadata.md](./metadata.md) | §12 | Catalog and schema metrics |
| [resilience.md](./resilience.md) | Observability § | Retry and circuit breaker metrics |
| [caching.md](./caching.md) | Observability § | Cache hit/miss and eviction metrics |
| [streaming.md](./streaming.md) | Observability § | Streaming ingestion metrics |
| [data-governance.md](./data-governance.md) | Observability § | Governance audit metrics |
| [security.md](./security.md) | Observability § | Authentication and authorization metrics |
| [multi-tenancy.md](./multi-tenancy.md) | Observability § | Tenant isolation metrics |

All metrics follow the `pangrova.*` naming convention defined in §3. All tracing spans follow the span schema defined in §4.




> **SPI Alignment Note**: TracingProvider and MetricsProvider signatures in §2.1 are authoritative. The [00 §5.1](./overview.md) SPI summary table has been updated to match (see SPI-C03, SPI-C04 fixes).


> **Configuration status:** `native.metrics.*` keys in this design are not part of
> the verified [configuration key reference](../configuration/reference.md).


> **Tracing sample rate:** The example value in §2.2 is illustrative. No
> `tracing.sampling-rate` key is currently exposed through the typed YAML options;
> do not treat this document as a runtime default.

---

## Appendix: Metrics Naming Conventions

### General Rules

All Pangrova metrics use the `pangrova_` prefix (Prometheus) or `pangrova.` prefix (OTel SDK). Naming structure: `pangrova.<component>.<entity>.<action>[.<detail>]`.

| Convention | OTel SDK | Prometheus |
|-----------|---------|------------|
| Separator | `.` (dot) | `_` (underscore) |
| Prefix | `pangrova.` | `pangrova_` |
| Counter suffix | — | `_total` (auto by OTel exporter) |
| Unit suffix | — | `_seconds`, `_bytes` (auto) |

### Instrument Selection

| Use Case | Instrument | Example |
|----------|-----------|---------|
| Monotonic count | Counter | `pangrova.coordinator.queries.submitted` |
| Request latency | Histogram | `pangrova.coordinator.query.duration` |
| Current value | Gauge | `pangrova.worker.memory.used_bytes` |
| Non-monotonic count | UpDownCounter | `pangrova.worker.tasks.active` |

### Component Metrics

**Coordinator**: `queries.submitted`, `queries.completed`, `queries.failed`, `queries.running`, `query.duration`, `planning.duration`, `scheduling.duration`

**Worker**: `tasks.submitted`, `tasks.completed`, `tasks.failed`, `memory.used_bytes`, `memory.peak_bytes`, `io.read_bytes`, `io.read_rows`, `shuffle.write_bytes`

**Native**: `velox.pipeline.duration`, `velox.operator.duration`, `velox.memory.current_bytes`, `velox.spill.bytes`

**Cache**: `cache.hits`, `cache.misses`, `cache.evictions`, `cache.size_bytes`

### Label Conventions

| Label | Scope | Values |
|-------|-------|--------|
| `query_id` | per-query | UUID |
| `task_id` | per-task | `{queryId}-{stageId}-{partitionId}` |
| `worker_id` | per-worker | hostname or configured ID |
| `tenant_id` | per-tenant | tenant identifier |
| `error_code` | per-error | `PangrovaErrorCode` string |

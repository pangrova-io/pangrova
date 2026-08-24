---
title: Observability
sidebar_label: Observability
---

# Pangrova — Observability

> **Summary.** The OpenTelemetry + Prometheus observability model — distributed tracing, metrics, and structured logs.

## 1. Overall Architecture

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
// <!-- Resolves: DEF-183 — LoggingProvider SPI should be registered in 00 §5.1 SPI summary table alongside TracingProvider and MetricsProvider to complete the Observability SPI trio. -->
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

<!-- Resolves: DEF-189 — Split mixed YAML/Java code block into separate blocks with correct language tags. -->

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
<!-- Resolves: DEF-185 — Renumbered from duplicate §4.2 to §4.3; subsequent sections cascade §4.3→§4.4, §4.4→§4.5 -->

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
│  Storage: PostgreSQL (query_audit_records table), retained for 30 days              │
│  Purpose: cost allocation, capacity planning, anomalous-query detection, SLA reporting │

<!-- Resolves: DEF-AP-011, DEF-AP-013 — Audit boundary clarification.
     Security audit events (authentication, authorization, DDL, data access) are owned by 15-security.md §6.
     Resource audit events (CPU, memory, I/O, shuffle) are owned by this section (14 §4.4).
     Both write to a unified audit_events table via shared AuditEvent base schema (see 12 §6.3).
     Field naming alignment: wallTimeMs maps to 12's durationMs; inputRows maps to 12's rowsRead. -->
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

## 5. Native-Layer Metrics

### 5.1 Native Metric Definitions

| Metric Name | Type | Description |
|--------|------|------|
<!-- Resolves: DEF-186 — Unified metric naming to dot-separated, removed unit suffixes per OpenTelemetry Semantic Conventions. Units specified via instrument unit parameter. -->
<!-- Resolves: DEF-187 — Added ReadStats metrics (row.groups, bloom.filter, delete.rows) aligned with 03 §5.3 TaskStats Protobuf. Removed spill.count (no corresponding TaskStats field; derive from spill.bytes > 0 transitions in Java layer). Added io.output.bytes. -->
<!-- Resolves: DEF-190 — Clarified memory.allocated data source: provided by Java MemoryManager, not via Native TaskStats Pull (03 §5.3 TaskStats has no allocated_memory field). -->
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

The Java layer actively pulls Native metrics via `nativeGetTaskStats()` (see [05-native-layer.md §5.1](./05-native-layer.md) NativeRuntime). This section defines the pull-frequency contract and backpressure behavior.

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

**Configuration** (see [21-configuration-reference.md](./21-configuration-reference.md)):

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
pangrova-observability/
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


<!-- Resolves: DEF-188 — Integration with 12 §6 audit and 11 §3 OpenLineage -->
### 8.5 Audit and Lineage Integration

### Security Audit vs Resource Audit

Pangrova has two complementary audit subsystems:

| Aspect | Security Audit ([12 §6](./15-security.md)) | Resource Audit (§4.5) |
|--------|---------------------------------------------|----------------------|
| Scope | Who did what (authentication, authorization, DDL, data access) | How much was consumed (CPU, memory, I/O, shuffle) |
| Storage | `audit_logs` table | `query_audit_records` table |
| Key fields | principal, clientIp, sql, success, accessedObjects | cpuTimeMs, peakMemoryBytes, spilledBytes, inputRows |
| Owner | SecurityModule | ObservabilityModule |

Both tables share a common `AuditEvent` base schema (see [12 §6.3](./15-security.md)) and can be joined via `query_id` for full audit trail.

### OpenLineage Trace Correlation

Data lineage events emitted by [11 §3 LineageEventEmitter](./17-data-governance.md) should include `traceId` in the OpenLineage `RunEvent.run.facets` to enable lineage-to-trace correlation:

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

Observability alerting is defined in [22-operations.md](./22-operations.md). Security components (e.g., [12 §10.6](./15-security.md) "3 consecutive failures → CRITICAL alert") emit alerts through the observability pipeline defined in §3 metrics + §7 logging.


<!-- Resolves: DEF-AP-014 — Cross-reference index for observability consumers -->
## 9. Cross-Module Observability Reference

This document (14-observability.md) is the authoritative source for the observability framework. The following architecture documents contain module-specific Observability Integration sections that reference this document:

| Document | Section | Coverage |
|----------|---------|----------|
| [02-distributed-architecture.md](./02-distributed-architecture.md) | §7 | Cluster-level metrics and tracing |
| [03-query-execution.md](./03-query-execution.md) | §16 | Query lifecycle metrics and tracing |
| [05-native-layer.md](./05-native-layer.md) | §18 | Native operator and JNI metrics |
| [06-connector-design.md](./06-connector-design.md) | Observability §  | Connector I/O metrics |
| [04-query-optimization.md](./04-query-optimization.md) | Observability § | Optimizer statistics metrics |
| [11-resource-management.md](./11-resource-management.md) | Observability § | Memory and resource pool metrics |
| [07-metadata-management.md](./07-metadata-management.md) | §12 | Catalog and schema metrics |
| [12-resilience.md](./12-resilience.md) | Observability § | Retry and circuit breaker metrics |
| [13-cache-layer.md](./13-cache-layer.md) | Observability § | Cache hit/miss and eviction metrics |
| [09-streaming.md](./09-streaming.md) | Observability § | Streaming ingestion metrics |
| [17-data-governance.md](./17-data-governance.md) | Observability § | Governance audit metrics |
| [15-security.md](./15-security.md) | Observability § | Authentication and authorization metrics |
| [16-multi-tenancy.md](./16-multi-tenancy.md) | Observability § | Tenant isolation metrics |

All metrics follow the `pangrova.*` naming convention defined in §3. All tracing spans follow the span schema defined in §4.


<!-- Resolves: DEF-180 — TracingProvider SPI alignment with 00 §5.1 (also fixed by SPI-C03) -->
<!-- Resolves: DEF-181 — MetricsProvider SPI alignment with 00 §5.1 (also fixed by SPI-C04) -->
> **SPI Alignment Note**: TracingProvider and MetricsProvider signatures in §2.1 are authoritative. The [00 §5.1](./01-overview.md) SPI summary table has been updated to match (see SPI-C03, SPI-C04 fixes).

<!-- Resolves: DEF-182 — Native Metrics Pull config params missing from 16 -->
> **Configuration Cross-Reference (§5.3)**: Native metrics pull configuration parameters (`native.metrics.pull-interval`, `native.metrics.batch-size`, `native.metrics.enabled`) are registered in [21-configuration-reference.md §12.9](./21-configuration-reference.md).

<!-- Resolves: DEF-184 — tracingSampleRate default value contradiction -->
> **Tracing Sample Rate**: The authoritative default for `tracing.sampling-rate` is `0.01` (1%) as defined in [21-configuration-reference.md §12.9](./21-configuration-reference.md). The `OpenTelemetryConfig.tracingSampleRate = 1.0` in §2.2 represents the maximum value (100% sampling for development/debugging), not the production default. Production deployments should use the 16 §12.9 default of 0.01.

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

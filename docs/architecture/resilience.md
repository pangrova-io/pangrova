---
title: "Resilience"
sidebar_label: "Resilience"
---

# Pangrova — Resilience

> **Accuracy scope.** This is a living architecture document. Content explicitly marked
> draft, proposed, planned, phase, target, or remaining work is design intent rather
> than implemented support. For current behavior, code wiring and end-to-end tests
> take precedence.

> **Summary.** The error-code taxonomy, circuit breakers, graceful degradation, and retry policy.

## 1. Resilience Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         System Resilience Architecture                           │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  Resilience capabilities:                                                        │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  Capability         │ Description                 │ Implementation      │   │
│  │  ─────────────────────────────────────────────────────────────────────  │   │
│  │  Error Handling     │ Unified error codes & prop. │ Layered error model │   │
│  │  Circuit Breaker    │ Fault isolation & recovery  │ State machine       │   │
│  │  Degradation        │ Graceful degradation        │ Multi-level         │   │
│  │  Backpressure       │ Backpressure prop. & flow   │ Watermark control   │   │
│  │  Retry              │ Smart retry & idempotency   │ Exponential backoff │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Unified Error-Code System

> **Implementation note**: The production `PangrovaErrorCode` enum uses string-prefix codes
> (`CATEGORY-NNNN` format, e.g., `PARSER-0001`, `SECURITY-0001`, `NATIVE-0001`) — not the
> integer-range format shown in the code snippets below. The category grouping and retry
> semantics remain accurate; the code format has evolved from integer ranges to string prefixes.
> See `pangrova-common/.../exception/PangrovaErrorCode.java` for the authoritative enum.

### 2.1 Error Classification

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         Error Classification                                     │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  Error classification:                                                           │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  Category          │ Code range    │ Retry policy  │ Examples           │   │
│  │  ─────────────────────────────────────────────────────────────────────  │   │
│  │  USER_ERROR        │ 1000-1999     │ No retry      │ Syntax, no access  │   │
│  │  RESOURCE_ERROR    │ 2000-2999     │ Retryable     │ No resources, t/o  │   │
│  │  INTERNAL_ERROR    │ 3000-3999     │ No retry      │ Internal error, bug│   │
│  │  EXTERNAL_ERROR    │ 4000-4999     │ Retryable     │ Storage, network   │   │
│  │  EXCHANGE_ERROR    │ 5000-5999     │ Retryable     │ Upstream task, conn lost │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 2.2 Error-Code Definition

```java
/**
 * Pangrova unified error codes
 */
public enum PangrovaErrorCode {

    // ========== USER_ERROR (1000-1999): user errors, non-retryable ==========
    SYNTAX_ERROR(1001, "SQL syntax error"),
    TABLE_NOT_FOUND(1101, "Table not found"),
    COLUMN_NOT_FOUND(1102, "Column not found"),
    ACCESS_DENIED(1201, "Access denied"),

    // Schema Evolution errors (1300-1399)
    SCHEMA_INCOMPATIBLE(1301, "Schema change is incompatible"),
    COLUMN_TYPE_MISMATCH(1302, "Column type mismatch during schema evolution"),

    // CDC errors (1400-1499)
    CDC_PARSE_ERROR(1401, "Failed to parse CDC message"),
    CDC_OFFSET_ERROR(1402, "CDC offset commit failed"),
    //
    CDC_CONNECTION_FAILED(1403, "Kafka connection failed"),
    CDC_QUOTA_EXCEEDED(1404, "CDC consumer resource quota exceeded"),
    CDC_REBALANCE_FAILED(1405, "Kafka consumer group rebalance failed"),
    STREAMING_BUFFER_OVERFLOW(1410, "Streaming ingestion buffer overflow"),
    STREAMING_FLUSH_FAILED(1411, "Streaming flush to storage failed"),
    MAINTENANCE_TASK_TIMEOUT(1420, "Maintenance task exceeded timeout"),
    MAINTENANCE_COMPACTION_FAILED(1421, "Compaction task failed"),
    WATERMARK_INFERENCE_FAILED(1430, "Watermark inference failed — missing timestamp field or empty record set"),

    //
    // Cache errors (1500-1599) — internal system events, NOT user-facing errors
    // These codes are used for internal metrics/logging only; they are never
    // returned to clients. On CACHE_MISS the system transparently reads from
    // the data source. On CACHE_INVALIDATION_FAILED the system logs a warning
    // and schedules a retry.
    CACHE_MISS(1501, "Cache entry not found"),              // internal-only, not returned to client
    CACHE_INVALIDATION_FAILED(1502, "Cache invalidation failed"), // internal-only, logged as warning

    // Multi-tenancy errors (1600-1699)
    TENANT_NOT_FOUND(1601, "Tenant not found"),
    TENANT_QUOTA_EXCEEDED(1602, "Tenant resource quota exceeded"),
    TENANT_ACCESS_DENIED(1603, "Tenant access denied to catalog"),

    //
    // Metadata errors (1700-1799)
    CATALOG_NOT_FOUND(1701, "Catalog not found"),
    CATALOG_ALREADY_EXISTS(1702, "Catalog already exists"),
    CATALOG_SUSPENDED(1703, "Catalog is suspended"),
    CATALOG_PROVIDER_NOT_FOUND(1704, "Catalog provider not found"),
    CATALOG_BACKEND_ERROR(1705, "Catalog backend unreachable"),
    METASTORE_ERROR(1706, "Internal metastore persistence error"),
    METADATA_SERIALIZATION_ERROR(1707, "Metadata serialization/deserialization error"),

    //
    // Security errors (1800-1899)
    AUTHENTICATION_FAILED(1801, "Authentication failed"),
    TOKEN_EXPIRED(1802, "Authentication token expired"),
    RLS_POLICY_ERROR(1803, "Row-level security policy evaluation error"),
    COLUMN_MASK_ERROR(1804, "Column masking policy evaluation error"),
    AUDIT_WRITE_FAILED(1805, "Audit log write failed"),

    //
    // Governance errors (1850-1899)
    DATA_QUALITY_CHECK_FAILED(1851, "Data quality check failed"),
    DATA_QUALITY_RULE_INVALID(1852, "Data quality rule expression invalid"),
    LINEAGE_EMISSION_FAILED(1853, "OpenLineage event emission failed"),
    CLASSIFICATION_TAG_INVALID(1854, "Data classification tag invalid"),

    // ========== RESOURCE_ERROR (2000-2999): resource errors, retryable ==========
    RESOURCE_EXHAUSTED(2001, "Resource exhausted"),
    MEMORY_EXCEEDED(2002, "Memory limit exceeded"),
    QUERY_TIMEOUT(2101, "Query execution timeout"),
    QUERY_CANCELLED(2201, "Query was cancelled"),

    // ========== INTERNAL_ERROR (3000-3999): internal errors, non-retryable ==========
    // [DSC-011 resolved] INTERNAL_ERROR is NOT retryable — these indicate bugs
    // that should fail immediately and be logged for investigation.
    INTERNAL_ERROR(3001, "Internal error"),
    NATIVE_ERROR(3101, "Native execution error"),
    JNI_ERROR(3102, "JNI bridge error"),

    // ========== EXTERNAL_ERROR (4000-4999): external errors, retryable ==========
    STORAGE_ERROR(4001, "Storage access error"),
    NETWORK_ERROR(4101, "Network error"),
    COORDINATOR_UNAVAILABLE(4203, "Coordinator unavailable"),
    WORKER_UNAVAILABLE(4204, "Worker unavailable"),

    // ========== EXCHANGE_ERROR (5000-5999): data exchange errors, retryable ==========
    UPSTREAM_TASK_FAILED(5001, "Upstream task failed"),
    EXCHANGE_CONNECTION_LOST(5101, "Exchange connection lost"),
    EXCHANGE_DATA_TIMEOUT(5102, "Exchange data fetch timeout");

    private final int code;
    private final String message;

    /**
     * Retryable error codes are RESOURCE_ERROR (2000-2999),
     * EXTERNAL_ERROR (4000-4999), and EXCHANGE_ERROR (5000-5999).
     *
     * USER_ERROR (1000-1999) and INTERNAL_ERROR (3000-3999) are NOT retryable.
     * [DSC-011 resolved] INTERNAL_ERROR was previously marked retryable;
     * aligned with query-execution.md — internal errors (bugs) must fail
     * immediately and be logged for investigation, not retried.
     */
    public boolean isRetryable() {
        return (code >= 2000 && code < 3000) || code >= 4000;
    }
}
```

### 2.3 Exception Class Hierarchy

```java
/**
 * Pangrova base exception class
 */
public class PangrovaException extends RuntimeException {
    private final PangrovaErrorCode errorCode;
    private final Map<String, Object> context;
    private final String traceId;

    public boolean isRetryable() {
        return errorCode.isRetryable();
    }

    public ErrorResponse toErrorResponse() {
        return ErrorResponse.builder()
            .errorCode(errorCode.getCode())
            .errorType(errorCode.name())
            .message(getMessage())
            .traceId(traceId)
            .context(sanitizeContext(context))
            .build();
    }
}
```

---

## 3. Java ↔ Native Exception Propagation

### 3.1 Propagation Flow

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                    Java ↔ Native Exception Propagation                           │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  Propagation flow:                                                               │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  Java Layer                                                              │   │
│  │       │ JNI Call                                                        │   │
│  │       ▼                                                                  │   │
│  │  ┌─────────────────────────────────────────────────────────────────┐   │   │
│  │  │  Native Layer (C++)                                              │   │   │
│  │  │  1. Catch C++ Exception                                          │   │   │
│  │  │  2. Convert to NativeError (Protobuf)                           │   │   │
│  │  │  3. Store error in TaskState, mark task FAILED                  │   │   │
│  │  └─────────────────────────────────────────────────────────────────┘   │   │
│  │       │ Java Pull                                                       │   │
│  │       ▼                                                                  │   │
│  │  4. nativeGetNextBatch() returns < 0 (error code)                       │   │
│  │  5. Java calls nativeGetTaskStatus() to fetch NativeError details       │   │
│  │  6. Convert to PangrovaException, attach Java-layer context (TraceId, QueryId) │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 3.2 Native Error Protocol



```protobuf
// Authoritative definition: native-execution.md §5.3 TaskStatus.error_detail
message NativeError {
    int32 error_code = 1;
    string error_type = 2;
    string message = 3;
    string stack_trace = 4;
    string task_id = 5;
    string operator_type = 6;
    int32 operator_id = 7;
    int64 rows_processed = 8;
    map<string, string> properties = 9;
}
```

### 3.3 Native → Java Error-Code Mapping

The Native layer's `NativeError.error_code` is mapped to the Java layer's `PangrovaErrorCode` according to the following rules:

| Native error_code range | Native error_type | PangrovaErrorCode | Description |
|---|---|---|---|
| 100-199 | `MEMORY_ERROR` | `MEMORY_EXCEEDED` (2002) | Velox memory allocation failure, spill failure |
| 200-299 | `IO_ERROR` | `STORAGE_ERROR` (4001) | File read failure, S3/HDFS unreachable |
| 300-399 | `FORMAT_ERROR` | `INTERNAL_ERROR` (3001) | Parquet/ORC format parse error |
| 400-499 | `EXPRESSION_ERROR` | `INTERNAL_ERROR` (3001) | Expression evaluation error, type conversion failure |
| 500-599 | `OPERATOR_ERROR` | `INTERNAL_ERROR` (3001) | Operator execution error (Join/Agg/Sort) |
| 600-699 | `SCHEMA_ERROR` | `INTERNAL_ERROR` (3001) | Schema Evolution mapping failure |
| 700-799 | `CANCEL_ERROR` | `QUERY_CANCELLED` (2201) | Task cancelled |
| 800-899 | `SPILL_ERROR` | `STORAGE_ERROR` (4001) | Spill write/read failure (disk full, IO error) |
| 900-999 | `UNKNOWN_ERROR` | `NATIVE_ERROR` (3101) | Unclassified Native error |

> **Design note:** The mapping table below is a target consolidation. Current Java
> error handling uses `PangrovaErrorCode`, `PangrovaException`, and transport-specific
> mapping such as `TransportStatusMapper`; there is no standalone native error mapper.

Mapping implementation:

```java
/**
 * Native error code → PangrovaErrorCode mapping
 *
 * Mapping rules:
 * - Classify by error_code / 100; each hundreds bucket maps to one Native error type
 * - Unregistered ranges are uniformly mapped to NATIVE_ERROR (3101)
 * - The mapping table must stay in sync with the Native-layer error_code definitions
 */
public class NativeErrorMapper {

    public static PangrovaErrorCode map(int nativeErrorCode) {
        int category = nativeErrorCode / 100;
        return switch (category) {
            case 1 -> PangrovaErrorCode.MEMORY_EXCEEDED;       // 100-199: Memory
            case 2 -> PangrovaErrorCode.STORAGE_ERROR;         // 200-299: IO
            case 3, 4, 5, 6 -> PangrovaErrorCode.INTERNAL_ERROR; // 300-699: Format/Expr/Op/Schema
            case 7 -> PangrovaErrorCode.QUERY_CANCELLED;       // 700-799: Cancel
            case 8 -> PangrovaErrorCode.STORAGE_ERROR;         // 800-899: Spill IO
            default -> PangrovaErrorCode.NATIVE_ERROR;         // 900-999 + unknown
        };
    }

    /**
     * Extract detailed error context for diagnostics.
     * Enriches PangrovaException with Native-specific information.
     */
    public static Map<String, String> extractContext(NativeError nativeError) {
        Map<String, String> ctx = new LinkedHashMap<>();
        ctx.put("native_error_code", String.valueOf(nativeError.getErrorCode()));
        ctx.put("native_error_type", nativeError.getErrorType());
        ctx.put("operator_type", nativeError.getOperatorType());
        ctx.put("operator_id", String.valueOf(nativeError.getOperatorId()));
        ctx.put("rows_processed", String.valueOf(nativeError.getRowsProcessed()));
        ctx.putAll(nativeError.getPropertiesMap());
        return ctx;
    }
}
```

---

### 3.4 PangrovaErrorCode → HTTP Status Code Mapping

The HTTP status code returned by the REST API is determined by the PangrovaErrorCode category:

| PangrovaErrorCode range | ErrorCategory | HTTP Status Code | Description |
|---|---|---|---|
| 1000-1099 | USER_ERROR (syntax) | 400 Bad Request | SQL syntax error |
| 1100-1199 | USER_ERROR (object) | 404 Not Found | Table/column/schema not found |
| 1200-1299 | USER_ERROR (permission) | 403 Forbidden | Insufficient permissions |
| 1300-1499 | USER_ERROR (Schema/CDC) | 400 Bad Request | Schema incompatible, CDC parse error |
| 1500-1599 | USER_ERROR (cache) | — (internal only) | Cache miss/invalidation — internal metrics only, never returned to client (transparent fallback to data source) |
| 1600-1699 | USER_ERROR (tenant) | 403 Forbidden / 429 Too Many Requests | Tenant not found / quota exceeded |
| 1700-1799 | USER_ERROR (metadata) | 404 Not Found / 500 Internal Server Error | Catalog/Metadata error |
| 1800-1899 | USER_ERROR (security) | 401 Unauthorized / 403 Forbidden | Authentication failure / RLS policy error |
| 2001-2002 | RESOURCE_ERROR | 503 Service Unavailable | Resource exhausted, memory exceeded |
| 2101 | RESOURCE_ERROR (timeout) | 504 Gateway Timeout | Query execution timeout |
| 2201 | RESOURCE_ERROR (cancel) | 499 Client Closed Request | Query cancelled |
| 3000-3999 | INTERNAL_ERROR | 500 Internal Server Error | Internal error, Native error |
| 4000-4999 | EXTERNAL_ERROR | 502 Bad Gateway | Storage/network/upstream service error |
| 5000-5999 | EXCHANGE_ERROR | 502 Bad Gateway | Data exchange error |

Mapping implementation:

```java
public class HttpStatusMapper {

    public static int toHttpStatus(PangrovaErrorCode errorCode) {
        int code = errorCode.getCode();
        if (code >= 1000 && code < 1100) return 400;  // Syntax
        if (code >= 1100 && code < 1200) return 404;  // Not Found
        if (code >= 1200 && code < 1300) return 403;  // Forbidden
        if (code >= 1300 && code < 1500) return 400;  // Bad Request
        // 1500-1599: Cache errors — internal only, should not reach HTTP layer.
        // If they do, return 500 as a safety fallback.
        if (code >= 1500 && code < 1600) return 500;  // Cache (internal)
        if (code == 1602) return 429;                  // Tenant Quota
        if (code >= 1600 && code < 1700) return 403;  // Tenant Access
        if (code >= 1700 && code < 1800) return 404;  // Metadata/Catalog
        //
        if (code == 1801 || code == 1802) return 401;  // Authentication
        if (code >= 1803 && code < 1900) return 403;  // Authorization/RLS
        if (code == 2101) return 504;                  // Timeout
        if (code == 2201) return 499;                  // Cancelled
        if (code >= 2000 && code < 3000) return 503;  // Resource
        if (code >= 3000 && code < 4000) return 500;  // Internal
        if (code >= 4000 && code < 6000) return 502;  // External/Exchange
        return 500;                                    // Unknown
    }
}
```

REST API error response example (the JSON format in §8.1 is unchanged; the HTTP status code is determined by the table above):
```
HTTP/1.1 404 Not Found
Content-Type: application/json

{
    "errorCode": 1101,
    "errorType": "TABLE_NOT_FOUND",
    "message": "Table 'iceberg.db.orders' not found",
    ...
}
```

---

### 3.5 PangrovaErrorCode → Arrow Flight CallStatus Mapping



Arrow Flight (gRPC) interactions use `CallStatus` codes instead of HTTP status codes. The Flight Server in Worker nodes maps Pangrova error categories to Arrow Flight `CallStatus` as follows:

| ErrorCategory | Arrow Flight CallStatus | gRPC Code | Usage Context |
|---|---|---|---|
| USER_ERROR (1000-1999) | `INVALID_ARGUMENT` | 3 | SQL errors, schema mismatch, permission denied |
| RESOURCE_ERROR (2000-2999) | `RESOURCE_EXHAUSTED` | 8 | OOM, query timeout |
| RESOURCE_ERROR (2201 cancel) | `CANCELLED` | 1 | Query cancelled by user |
| INTERNAL_ERROR (3000-3999) | `INTERNAL` | 13 | Bugs, native errors, JNI errors |
| EXTERNAL_ERROR (4000-4999) | `UNAVAILABLE` | 14 | Storage errors, upstream service unavailable |
| EXCHANGE_ERROR (5000-5999) | `UNAVAILABLE` | 14 | Upstream task failed, exchange connection lost |

The target Flight mapping carries structured error detail in
`CallStatus.description()`. Current REST responses use `ErrorDetail`, while Java
domain failures carry `PangrovaErrorCode` through `PangrovaException`.

```java
// Flight Server error mapping (Worker-side)
public static CallStatus toFlightStatus(PangrovaErrorCode errorCode, String message) {
    int code = errorCode.getCode();
    if (code == 2201) return CallStatus.CANCELLED.withDescription(message);
    if (code >= 1000 && code < 2000) return CallStatus.INVALID_ARGUMENT.withDescription(message);
    if (code >= 2000 && code < 3000) return CallStatus.RESOURCE_EXHAUSTED.withDescription(message);
    if (code >= 3000 && code < 4000) return CallStatus.INTERNAL.withDescription(message);
    if (code >= 4000 && code < 6000) return CallStatus.UNAVAILABLE.withDescription(message);
    return CallStatus.INTERNAL.withDescription(message);
}
```

---

## 4. Circuit Breaker Design

### 4.1 Circuit Breaker State Machine

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         Circuit Breaker State Machine                            │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  State transitions:                                                              │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │                                                                          │   │
│  │     ┌──────────┐  failure rate>thr.  ┌──────────┐                       │   │
│  │     │  CLOSED  │ ─────────────────► │   OPEN   │                       │   │
│  │     │ (normal) │                     │ (tripped)│                       │   │
│  │     └────┬─────┘                     └────┬─────┘                       │   │
│  │          │                                │                              │   │
│  │          │ success                        │ after timeout                │   │
│  │          │                                ▼                              │   │
│  │          │                          ┌──────────┐                        │   │
│  │          └────────────────────────  │HALF_OPEN │                        │   │
│  │                    success           │ (probing)│                        │   │
│  │                                      └────┬─────┘                        │   │
│  │                                           │ failure                      │   │
│  │                                           └──────► OPEN                 │   │
│  │                                                                          │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
│  Circuit breaker dimensions:                                                     │
│  • Worker level: trip when a single Worker fails                                 │
│  • Connector level: trip when a data source fails                                │
│  • External-service level: trip when Catalog/Metastore fails                     │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 4.2 Circuit Breaker Implementation

```java
/**
 * Circuit breaker
 */
public class CircuitBreaker {

    private final String name;
    private final CircuitBreakerConfig config;
    private final AtomicReference<State> state = new AtomicReference<>(State.CLOSED);
    private final SlidingWindowCounter failureCounter;

    public enum State { CLOSED, OPEN, HALF_OPEN }

    public <T> T execute(Supplier<T> operation) {
        if (!allowRequest()) {
            throw new CircuitBreakerOpenException(name, getTimeUntilRetry());
        }

        try {
            T result = operation.get();
            onSuccess();
            return result;
        } catch (Exception e) {
            onFailure(e);
            throw e;
        }
    }

    private void onSuccess() {
        if (state.get() == State.HALF_OPEN) {
            if (state.compareAndSet(State.HALF_OPEN, State.CLOSED)) {
                failureCounter.reset();
            }
        }
        failureCounter.recordSuccess();
    }

    private void onFailure(Exception e) {
        failureCounter.recordFailure();

        if (state.get() == State.HALF_OPEN) {
            state.compareAndSet(State.HALF_OPEN, State.OPEN);
            return;
        }

        //
        if (state.get() == State.CLOSED) {
            if (failureCounter.getTotalCalls() < config.getMinimumNumberOfCalls()) {
                return; // Not enough samples to evaluate failure rate
            }
            double failureRate = failureCounter.getFailureRate();
            if (failureRate >= config.getFailureRateThreshold()) {
                state.compareAndSet(State.CLOSED, State.OPEN);
            }
        }
    }
}
```

### 4.3 Circuit Breaker Configuration

```java
public class CircuitBreakerConfig {
    private final double failureRateThreshold = 0.5;      // 50% failure rate triggers trip
    private final int minimumNumberOfCalls = 10;
    private final Duration waitDurationInOpenState = Duration.ofSeconds(30);
    private final int permittedNumberOfCallsInHalfOpenState = 3;
    private final Duration slidingWindowSize = Duration.ofMinutes(1);
}
```

---

## 5. Degradation Strategy

### 5.1 Degradation Scenarios

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         Degradation Strategy                                     │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  Degradation scenarios:                                                          │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  Scenario            │ Degradation strategy      │ User impact          │   │
│  │  ─────────────────────────────────────────────────────────────────────  │   │
│  │  Insufficient Worker │ Limit concurrency, queue  │ Increased latency    │   │
│  │  Memory pressure     │ Reject large, allow small │ Large queries fail   │   │
│  │  Catalog unavailable │ Use cached metadata       │ Possible data staleness │ │
│  │  Partial Worker fail │ Reschedule to healthy WKr │ Increased latency    │   │
│  │  High storage latency│ Increase t/o, cut concurr.│ Increased latency    │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 5.2 Degradation Manager

```java
/**
 * Degradation manager
 */
public class DegradationManager {

    private final AtomicReference<DegradationLevel> currentLevel =
        new AtomicReference<>(DegradationLevel.NONE);

    public enum DegradationLevel {
        NONE,           // normal
        LIGHT,          // light degradation: limit large queries
        MODERATE,       // moderate degradation: limit concurrency, queue
        SEVERE,         // severe degradation: read-only, reject writes
        CRITICAL        // critical degradation: allow system queries only
    }

    @Scheduled(fixedRate = 5000)
    public void evaluateDegradation() {
        SystemHealth health = healthChecker.check();

        //
        DegradationLevel newLevel;
        if (health.getAvailableWorkerRatio() < 0.1 || health.getMemoryPressure() > 0.95) {
            newLevel = DegradationLevel.CRITICAL;
        } else if (health.getAvailableWorkerRatio() < 0.3) {
            newLevel = DegradationLevel.SEVERE;
        } else if (health.getMemoryPressure() > 0.9) {
            newLevel = DegradationLevel.MODERATE;
        } else if (health.getAvailableWorkerRatio() < 0.7 ||
                   health.getMemoryPressure() > 0.7) {
            newLevel = DegradationLevel.LIGHT;
        } else {
            newLevel = DegradationLevel.NONE;
        }

        DegradationLevel oldLevel = currentLevel.getAndSet(newLevel);
        if (oldLevel != newLevel) {
            notifyDegradationChange(oldLevel, newLevel);
        }
    }

    public void checkQueryAllowed(QueryRequest request) {
        DegradationLevel level = currentLevel.get();

        switch (level) {
            case CRITICAL:
                if (!request.isSystemQuery()) {
                    throw new DegradationException("Only system queries allowed");
                }
                break;
            case SEVERE:
                if (request.isWriteQuery()) {
                    throw new DegradationException("Write queries not allowed");
                }
                break;
            case MODERATE:
                if (isLargeQuery(request)) {
                    throw new DegradationException("Large queries not allowed");
                }
                break;
        }
    }
}
```

---

## 6. Backpressure Control

```java
/**
 * Backpressure controller
 */
public class BackpressureController {

    private final AtomicInteger pendingRequests = new AtomicInteger(0);
    private final int maxPendingRequests;
    private final int highWaterMark;
    private final int lowWaterMark;

    public BackpressureResult checkBackpressure() {
        int pending = pendingRequests.get();

        if (pending >= maxPendingRequests) {
            return BackpressureResult.reject("Too many pending requests");
        }

        if (pending >= highWaterMark) {
            return BackpressureResult.slowDown(calculateDelay(pending));
        }

        return BackpressureResult.proceed();
    }

    private Duration calculateDelay(int pending) {
        double ratio = (double)(pending - lowWaterMark) / (highWaterMark - lowWaterMark);
        long delayMs = (long)(ratio * 1000);
        return Duration.ofMillis(delayMs);
    }
}
```

---

## 7. Retry Strategy

### 7.1 Retry Configuration

```java
/**
 * Retry strategy configuration
 */
public class RetryPolicy {
    private final int maxRetries = 3;
    private final Duration initialBackoff = Duration.ofMillis(100);
    private final Duration maxBackoff = Duration.ofSeconds(10);
    private final double backoffMultiplier = 2.0;
    private final Set<PangrovaErrorCode> retryableErrors = Set.of(
        RESOURCE_EXHAUSTED, QUERY_TIMEOUT, NETWORK_ERROR,
        COORDINATOR_UNAVAILABLE, WORKER_UNAVAILABLE
    );

    public boolean shouldRetry(PangrovaException e, int attemptCount) {
        if (attemptCount >= maxRetries) return false;
        if (!e.isRetryable()) return false;
        return retryableErrors.contains(e.getErrorCode());
    }

    public Duration getBackoff(int attemptCount) {
        long backoffMs = (long) (initialBackoff.toMillis() *
                                  Math.pow(backoffMultiplier, attemptCount));
        return Duration.ofMillis(Math.min(backoffMs, maxBackoff.toMillis()));
    }
}
```

### 7.2 Idempotency Design

```java
/**
 * Idempotency manager
 */
public class IdempotencyManager {

    private final IdempotencyStore store;
    private final Duration keyTtl = Duration.ofHours(24);

    public <T> T executeIdempotent(String idempotencyKey, Supplier<T> operation) {
        Optional<CachedResult<T>> cached = store.get(idempotencyKey);
        if (cached.isPresent()) {
            if (cached.get().isSuccess()) {
                return cached.get().getResult();
            } else {
                throw cached.get().getException();
            }
        }

        try {
            T result = operation.get();
            store.put(idempotencyKey, CachedResult.success(result), keyTtl);
            return result;
        } catch (Exception e) {
            store.put(idempotencyKey, CachedResult.failure(e), keyTtl);
            throw e;
        }
    }
}
```



### 7.3 Target Shuffle S3 Recovery Path

This fallback is proposed and is not wired into the current worker exchange path.
The proposed historical design used an `ExchangeClient` abstraction to fall back to the S3
persistence layer:

1. The proposed exchange client fails to fetch its Flight ticket.
2. It checks whether remote shuffle persistence is enabled.
3. If enabled, it delegates to the proposed fault-tolerant shuffle reader.
4. A proposed `FaultTolerantShuffleReader` reads Arrow IPC bytes from S3 and deserializes it to `VectorSchemaRoot`
5. The recovered `VectorSchemaRoot` is injected into the Native Exchange Operator via the same `nativeAddExchangeBatch()` JNI call used by the normal path
6. From the Native layer's perspective, the data source (Flight vs S3) is transparent

This fallback is attempted once per source task. If S3 recovery also fails, the error escalates to task-level retry (§7.1).

---

## 8. Client Error Responses

### 8.1 Error Response Format

```json
{
    "errorCode": 1101,
    "errorType": "TABLE_NOT_FOUND",
    "message": "Table 'iceberg.db.orders' not found",
    "traceId": "abc123def456",
    "queryId": "query-789",
    "context": {
        "catalog": "iceberg",
        "schema": "db",
        "table": "orders"
    },
    "suggestion": "Check if the table exists using 'SHOW TABLES IN iceberg.db'",
    "documentationUrl": "https://docs.pangrova.io/errors/TABLE_NOT_FOUND"
}
```

### 8.2 Error Suggestion Generation

```java
public class ErrorSuggestionGenerator {

    public static String generateSuggestion(PangrovaException e) {
        return switch (e.getErrorCode()) {
            case TABLE_NOT_FOUND ->
                "Check if the table exists using 'SHOW TABLES'. " +
                "If recently created, try refreshing the catalog.";
            case ACCESS_DENIED ->
                "Contact your administrator to request access.";
            case MEMORY_EXCEEDED ->
                "Try reducing query complexity or increasing memory limits.";
            case QUERY_TIMEOUT ->
                "Consider optimizing the query or increasing timeout.";
            default -> null;
        };
    }
}
```

---

## 9. Error Monitoring and Alerting

```java
/**
 * Error monitor
 */
public class ErrorMonitor {

    public void recordError(PangrovaException e) {
        metrics.counter("pangrova.errors.total")
            .tag("error_code", e.getErrorCode().name())
            .tag("retryable", String.valueOf(e.isRetryable()))
            .increment();

        if (shouldAlert(e)) {
            alertManager.alert(AlertLevel.ERROR, "High error rate detected",
                Map.of("errorCode", e.getErrorCode().name()));
        }
    }

    private boolean shouldAlert(PangrovaException e) {
        // Alert immediately on internal errors
        if (e.getErrorCode().getCode() >= 3000 && e.getErrorCode().getCode() < 4000) {
            return true;
        }
        // Check error rate
        return getErrorRate(e.getErrorCode(), Duration.ofMinutes(5)) > 0.1;
    }
}
```

---

## 10. Configuration Summary

```yaml
# System resilience configuration
resilience:

  # Circuit breaker
  circuit-breaker:
    worker:
      failure-rate-threshold: 0.5
      minimum-calls: 10
      wait-duration-open: 30s
      permitted-calls-half-open: 3
    connector:
      failure-rate-threshold: 0.3
      wait-duration-open: 60s

  # Degradation
  degradation:
    enabled: true
    thresholds:
      light:
        worker-ratio: 0.7
        memory-pressure: 0.7
      moderate:
        memory-pressure: 0.9
      severe:
        worker-ratio: 0.3
      #
      critical:
        worker-ratio: 0.1
        memory-pressure: 0.95

  # Backpressure
  backpressure:
    max-pending-requests: 1000
    high-water-mark: 800
    low-water-mark: 500

  # Retry
  retry:
    max-retries: 3
    initial-backoff: 100ms
    max-backoff: 10s
    backoff-multiplier: 2.0
```

---



## 11. Observability Integration

This section defines the metrics and tracing integration points for the resilience layer. For the full observability design, see [observability.md](./observability.md).

### 11.1 Metrics

| Metric | Source Component | Description |
|--------|-----------------|-------------|

| `pangrova.errors.total` | ErrorClassifier | Total errors by category and retryable flag |
| `pangrova.resilience.retries.total` | RetryPolicy | Total retry attempts |
| `pangrova.resilience.retries.exhausted` | RetryPolicy | Retries exhausted (final failure) |
| `pangrova.resilience.circuit_breaker.state` | CircuitBreaker | Current state (CLOSED/OPEN/HALF_OPEN) |
| `pangrova.resilience.circuit_breaker.trips` | CircuitBreaker | Circuit breaker trip count |
| `pangrova.resilience.degradation.active` | DegradationManager | Active degradation count |

### 11.2 Tracing Spans

Resilience operations annotate existing spans with error and retry attributes:

- `task.execute` span receives `error.category`, `error.retryable`, `retry.attempt` attributes
- Circuit breaker state transitions are recorded as span events

### 11.3 Log Events

- Error classification events with ErrorCategory and error code (INFO)
- Retry attempt events with attempt number and backoff delay (INFO)
- Circuit breaker state transitions (WARN)
- Degradation activation/deactivation events (WARN)

See [14 §7](./observability.md) for structured logging format.



> **Retry Strategy Cross-Reference**: The retry behavior defined in §7.1 aligns with the error classification in [02 §3.3.5](./query-execution.md). Retryable categories: EXTERNAL_ERROR (max 3 retries), RESOURCE_ERROR (max 1 retry). Non-retryable: INTERNAL_ERROR, USER_ERROR. The `INTERNAL_ERROR` category is never retried because it indicates a bug in the engine — retrying would produce the same failure.


> **Circuit Breaker Clarification (§7.2)**: In HALF_OPEN state, the breaker allows up to `half-open-max-calls` (default: 3) probe requests. The breaker transitions to CLOSED only after ALL probe requests succeed. If any probe fails, it transitions back to OPEN. The code example showing single-success closure is simplified — the actual implementation tracks a success counter.


> **OPEN→HALF_OPEN Transition**: After entering OPEN state, a timer (`open-state-duration`, default: 60s) schedules the transition to HALF_OPEN. Implementation uses `ScheduledExecutorService.schedule()`. During OPEN state, all requests are immediately rejected with `CIRCUIT_OPEN` error code.


> **Exception Propagation (§3.1)**: Native layer exceptions are propagated via JNI as `NativeExecutionException` (see [03 §5.5](./native-execution.md)). The Java TaskRunner catches this exception, extracts the Velox error code, maps it to an Pangrova `ErrorCategory` using the mapping table in §7.3, and reports it via `TaskStatusReport`. The error code mapping is: `kOutOfMemory` → RESOURCE_ERROR, `kIOError` → EXTERNAL_ERROR, `kInvalidArgument` → USER_ERROR, all others → INTERNAL_ERROR.

---



## 12. Stage-Level Retry (OPT-012)

This section adds stage-level retry capability to the Coordinator's `StageScheduler`, triggered when split-level retry (§7) is insufficient — e.g., when shuffle data is lost due to multiple Worker crashes or rack-level failures.

### 12.1 Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                  Stage-Level Retry Flow                           │
│                                                                  │
│  Task Failure                                                    │
│       │                                                          │
│       ▼                                                          │
│  Split-Level Retry (§7)                                          │
│       │                                                          │
│       │ exhausted (retryCount >= maxSplitRetries)                │
│       ▼                                                          │
│  StageRetryManager.onTaskFailure()                               │
│       │                                                          │
│       ├─ failureRatio <= threshold? → failQuery()                │
│       │                                                          │
│       ├─ failureRatio > threshold AND retryable?                 │
│       │       │                                                  │
│       │       ▼                                                  │
│       │  initiateStageRetry()                                    │
│       │       ├─ Cancel running tasks                            │
│       │       ├─ Invalidate shuffle output                       │
│       │       ├─ Check upstream shuffle validity                 │
│       │       │       └─ Lost? → cascade upstream retry          │
│       │       └─ Re-schedule all tasks in stage                  │
│       │                                                          │
│       └─ stageRetryCount >= maxStageRetries? → failQuery()       │
└─────────────────────────────────────────────────────────────────┘
```

**Design Rationale**: Split-level retry handles individual task failures but cannot recover from systemic issues like lost shuffle data (Worker crash with `MEMORY_ONLY` mode) or rack-level network partitions. Stage-level retry re-executes the entire stage, including cascading to upstream stages whose shuffle output is no longer available.

### 12.2 Pseudocode

**Stage Retry Decision Logic**:

```pseudocode
class StageRetryManager:
    maxStageRetries = 2
    stageRetryTriggerThreshold = 0.3  // retry stage if > 30% of tasks fail

    function onTaskFailure(stage: Stage, task: Task, error: Error):
        if task.retryCount < maxSplitRetries:
            retrySplit(task)
            return

        stage.failedTaskCount++
        failureRatio = stage.failedTaskCount / stage.totalTasks

        if failureRatio > stageRetryTriggerThreshold
           AND stage.stageRetryCount < maxStageRetries
           AND isRetryableError(error):
            initiateStageRetry(stage)
        else:
            failQuery(stage.queryId, error)

    function initiateStageRetry(stage: Stage):
        stage.stageRetryCount++
        for task in stage.runningTasks:
            cancelTask(task)
        shuffleRegistry.invalidate(stage.queryId, stage.stageId)

        upstreamStages = findUpstreamStagesWithLostShuffle(stage)
        for upstream in upstreamStages:
            if NOT upstream.hasValidShuffleOutput():
                initiateStageRetry(upstream)

        stage.reset()
        stageScheduler.scheduleStage(stage)
```

**Shuffle Output Validation**:

```pseudocode
class ShuffleOutputValidator:
    function hasValidShuffleOutput(stage: Stage):
        for partition in stage.shufflePartitions:
            location = shuffleRegistry.getLocation(
                stage.queryId, stage.stageId, partition)
            if location == null:
                return false
            if NOT isWorkerAlive(location.workerId):
                if stage.shuffleMode == MEMORY_AND_REMOTE:
                    return s3ShuffleStore.exists(
                        stage.queryId, stage.stageId, partition)
                return false
        return true
```

### 12.3 Trade-offs

| Aspect | Split-Level Retry Only (Current) | With Stage-Level Retry |
|--------|----------------------------------|----------------------|
| Reliability | Query fails if split retry exhausted and shuffle data lost | Stage re-execution recovers from shuffle data loss |
| Performance | No re-computation overhead | Stage retry re-executes all tasks — potentially expensive for large stages |
| Complexity | Simple split-level retry | Medium — stage retry coordination, upstream cascade, shuffle invalidation |
| Risk | Query failure on systemic issues | Low — stage retry is last resort after split retry; bounded by maxStageRetries=2 |

### 12.4 Migration Path

| Phase | Scope | Description |
|-------|-------|-------------|
| 1 | Shuffle Output Validation | Proposed `ShuffleOutputValidator` detects lost shuffle data and logs validation results without triggering retry. |
| 2 | Leaf Stage Retry | Enable stage retry for source (scan) stages only — no upstream cascade needed. |
| 3 | Full Stage Retry with Cascade | Enable stage retry for intermediate stages with upstream cascade detection and re-execution. |

> **Cross-reference**: Stage-Level Retry depends on External Shuffle Service (OPT-010 in [02 §18](./query-execution.md)) for shuffle data validation — ESS ensures shuffle data survives Worker crash. Lineage-Based Recovery (OPT-017) extends this with a `RECOMPUTE` mode that skips S3 persistence entirely. Split-level retry configuration is defined in §7.


---



## 13. Lineage-Based Recovery Mode (OPT-017)

This section adds a `RECOMPUTE` shuffle recovery mode that skips S3 persistence and relies on re-executing upstream stages on failure. For short queries (< 30s), recomputation is cheaper than S3 upload overhead.

### 13.1 Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│              Shuffle Recovery Mode Selection                     │
│                                                                  │
│  Query Plan                                                      │
│       │                                                          │
│       ▼                                                          │
│  ShuffleRecoverySelector                                         │
│       │                                                          │
│       ├─ estimatedDuration < 30s? ──────▶ RECOMPUTE              │
│       │    (skip S3, rely on stage retry)                        │
│       │                                                          │
│       ├─ shuffleSize > 1GB? ────────────▶ MEMORY_AND_REMOTE     │
│       │    (S3 persistence worthwhile)                           │
│       │                                                          │
│       └─ cost-based comparison ─────────▶ cheaper mode           │
│                                                                  │
│  RECOMPUTE mode:                                                 │
│    On failure → StageLineageTracker.getRecomputeChain()          │
│    → re-execute upstream stages from source data                 │
└─────────────────────────────────────────────────────────────────┘
```

**Problem**: All queries pay S3 upload overhead regardless of duration. For a 5-second query, async S3 write adds unnecessary I/O and latency. Without `MEMORY_AND_REMOTE`, there is no recovery for lost shuffle data other than query failure.

### 13.2 Pseudocode

**Recovery Mode Selection**:

```pseudocode
class ShuffleRecoverySelector:
    recomputeThreshold = 30s
    costRatio = 2.0

    function selectMode(query: QueryPlan):
        if query.estimatedDuration() < recomputeThreshold:
            return RECOMPUTE
        if query.estimatedShuffleBytes() > 1GB:
            return MEMORY_AND_REMOTE

        recomputeCost = sum(query.stages.map(s => s.estimatedDuration))
        persistCost = query.estimatedShuffleBytes() / S3_WRITE_THROUGHPUT
        if recomputeCost < persistCost * costRatio:
            return RECOMPUTE
        return MEMORY_AND_REMOTE
```

**Lineage Tracking**:

```pseudocode
class StageLineageTracker:
    lineageGraph: Map<StageId, List<StageId>>

    function recordLineage(stage: Stage):
        lineageGraph.put(stage.stageId, stage.inputStages.map(s => s.stageId))

    function getRecomputeChain(failedStage: StageId):
        chain = []
        queue = [failedStage]
        while queue.nonEmpty:
            current = queue.removeFirst()
            if NOT hasValidOutput(current):
                chain.append(current)
                queue.addAll(lineageGraph.get(current))
        return chain.reversed()  // parents first
```

### 13.3 Trade-offs

| Aspect | S3 Persistence Only (Current) | With RECOMPUTE Mode |
|--------|------------------------------|---------------------|
| Performance | S3 upload overhead on all queries | Short queries skip S3; ~5–10% latency reduction for sub-30s queries |
| Reliability | Durable S3 recovery | RECOMPUTE trades durability for speed; acceptable for short queries |
| Complexity | Single recovery mode | Medium — mode selection, lineage tracking, stage re-execution |
| Cost | S3 PUT costs for all shuffle data | Reduced S3 costs for short queries |

### 13.4 Migration Path

| Phase | Scope | Description |
|-------|-------|-------------|
| 1 | Lineage Tracking | Coordinator records stage dependency graph. No behavior change. |
| 2 | RECOMPUTE Mode | Implement `RECOMPUTE` shuffle mode. Short queries auto-select. |
| 3 | Cost-Based Selection | Cost-based mode selection using CBO estimates. |

> **Cross-reference**: RECOMPUTE mode depends on Stage-Level Retry (§12 OPT-012) for stage re-execution. The lineage graph leverages the existing `StageScheduler` dependency tracking. Shuffle persistence modes are defined in [02 §6](./query-execution.md).


---



## 14. Operator-Level Checkpoint (OPT-023)

This section designs operator-level checkpointing for the two most expensive operators (HashBuild and Sort), enabling long-running queries to resume from the last checkpoint instead of re-executing the entire stage.

### 14.1 Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│              Operator-Level Checkpoint                            │
│                                                                  │
│  CheckpointTriggerPolicy                                         │
│    query running > 10 min?                                       │
│    operator state > 1GB?                                         │
│    time since last checkpoint > 5 min?                           │
│         │ YES                                                    │
│         ▼                                                        │
│  CheckpointableOperator.checkpoint(writer)                       │
│         │                                                        │
│         ▼                                                        │
│  NVMe SSD (local, fast write ~1–5 GB/s)                         │
│    ├─ HashBuild: hash table partitions (Arrow IPC)               │
│    └─ Sort: sorted run files + metadata                          │
│                                                                  │
│  On failure:                                                     │
│    CheckpointableOperator.restore(reader)                        │
│    → resume from last checkpoint, skip re-building state         │
└─────────────────────────────────────────────────────────────────┘
```

**Problem**: Query checkpoint (§7) captures stage/task state but not operator internal state (hash tables, sort buffers). On failure, the entire stage re-executes from scratch. For a 2-hour ETL query building a 50GB hash table, failure at 1h50m requires rebuilding the hash table from zero.

### 14.2 Pseudocode

**Checkpoint Interface**:

```pseudocode
interface CheckpointableOperator:
    function checkpoint(writer: CheckpointWriter): CheckpointHandle
    function restore(reader: CheckpointReader): void
    function estimateCheckpointSize(): long

class CheckpointWriter:
    target: NVMeSSD
    function writeBuffer(data: bytes): offset
    function writeMetadata(meta: OperatorCheckpointMeta): void
```

**HashBuild Checkpoint**:

```pseudocode
class HashBuildCheckpoint implements CheckpointableOperator:
    function checkpoint(writer: CheckpointWriter):
        meta = OperatorCheckpointMeta(
            operatorType = HASH_BUILD,
            rowCount = hashTable.rowCount,
            bucketCount = hashTable.bucketCount,
            buildComplete = hashTable.isComplete)
        for partition in hashTable.partitions:
            writer.writeBuffer(ArrowIPC.serialize(partition.data))
        writer.writeMetadata(meta)

    function restore(reader: CheckpointReader):
        meta = reader.readMetadata()
        hashTable = HashTable(meta.bucketCount)
        for partition in reader.readBuffers():
            hashTable.insertBatch(ArrowIPC.deserialize(partition))
        hashTable.isComplete = meta.buildComplete
```

**Checkpoint Trigger Policy**:

```pseudocode
class CheckpointTriggerPolicy:
    minOperatorSize = 1GB
    minQueryDuration = 600s     // 10 min
    checkpointInterval = 300s   // 5 min
    maxCheckpointSize = 10GB

    function shouldCheckpoint(operator: Operator, query: Query):
        if query.runningDuration < minQueryDuration: return false
        if operator.estimateCheckpointSize() < minOperatorSize: return false
        if operator.estimateCheckpointSize() > maxCheckpointSize: return false
        if timeSinceLastCheckpoint(operator) < checkpointInterval: return false
        return true
```

### 14.3 Trade-offs

| Aspect | Current (No Operator Checkpoint) | With Operator Checkpoint |
|--------|----------------------------------|--------------------------|
| Recovery time | Full stage re-execution (hours for large stages) | Resume from last checkpoint; recovery = time since checkpoint |
| Complexity | Simple — no operator state persistence | HIGH — deep Velox operator internals; serialization of hash tables |
| Risk | None | Medium — checkpoint consistency; NVMe SSD space consumption |
| Write overhead | None | ~0.2–1s per 1GB checkpoint to NVMe SSD |
| Applicability | N/A | Narrow — only benefits queries > 10 min with operators > 1GB state |

### 14.4 Migration Path

| Phase | Scope | Description |
|-------|-------|-------------|
| 1 | Checkpoint Interface | Proposed `CheckpointableOperator` interface; no implementation. |
| 2 | HashBuild Checkpoint | Implement checkpoint/restore for HashBuild. Test with synthetic long-running queries. |
| 3 | Sort Checkpoint | Implement checkpoint/restore for Sort (external sort with spill files). |
| 4 | Automatic Trigger | Proposed `CheckpointTriggerPolicy` for long-running queries. |

> **Cross-reference**: Operator checkpoint depends on Stage-Level Retry (§12 OPT-012) for stage re-execution coordination. Spill infrastructure (§6, [06 §6](./resource-management.md)) provides the NVMe SSD write path. This is a forward-looking capability — no mainstream batch engine supports operator-level checkpointing (Flink supports it for streaming only).

---

## Appendix: Distributed Correctness PR Checklist

CI enforces this checklist when PRs modify `pangrova-coordinator/`, `pangrova-worker/`, `pangrova-shuffle/`, or `pangrova-metadata/`.

### 1. Network Partition Behavior

What happens if a network partition occurs? Consider: Coordinator↔Worker, Worker↔Metadata, Worker↔Worker (shuffle). Every operation must have a defined timeout + error code + retry safety statement.

### 2. Dual-Leader Safety

Can concurrent execution on multiple coordinators (split-brain) cause data corruption? All metadata writes must use fencing tokens (monotonic epoch). Stale coordinators must be rejected with `FENCED_OUT`.

Execution ownership uses a globally unique PostgreSQL sequence token, and each replacement of the same execution ID must receive a strictly greater token. `CACHE 1000` allowed pooled backends to return reserved ranges out of order. Migration V78 drains anchor transactions, locks the sequence, and restarts it above both its reserved high watermark and every live anchor, with `CACHE 1 NO CYCLE`. Existing pooled sessions retain their identity and sequence permissions; the transactional storage rewrite discards their unused cached values. Exhaustion aborts the migration instead of wrapping or reusing tokens. PostgreSQL 16 also rewrites storage for a CACHE change; the explicit RESTART establishes the upgrade floor rather than assuming cache settings alone establish it. See [PostgreSQL sequence caching](https://www.postgresql.org/docs/16/sql-createsequence.html) and [the PostgreSQL 16 implementation](https://github.com/postgres/postgres/blob/REL_16_STABLE/src/backend/commands/sequence.c).

Acquiring an execution ID also takes a transaction-scoped advisory lock before inspecting its row or allocating a token. This serializes absent-row acquisition across release and reuse of that ID; row locks alone cannot prevent a delayed low-token INSERT after a newer owner has acquired and released the same ID. Different IDs remain independent apart from harmless hash collisions. The rollout must replace old Coordinator binaries before relying on this absent-row guarantee; the database migration cannot add the advisory-lock protocol to older code. Batch recovery continues to lock existing rows before issuing replacement tokens.

Query execution anchors are renewed in a batch for one exact Coordinator incarnation. Local admission and release may run concurrently, but the bulk renewal holds an exclusive boundary against those cache mutations. A changed batch row count alone is not evidence of lost ownership: the Coordinator checks each locally retained anchor's current fenced token before demoting. A genuinely missing or replaced token still fails closed.

### 3. Crash Recovery

If any node crashes mid-operation, does the system recover to a consistent state? Task state must be persisted to WAL before dispatch. Workers deduplicate by task ID + attempt number. Orphaned files cleaned by background GC.

QueryManager owns failure publication and physical cleanup. QueryDispatcher propagates
execution failures while retaining the metadata admission needed to record FAILED.
In particular, once an asynchronous write submission has returned, early dispatcher
cleanup can claim the drained execution scope and permanently close that admission;
the original exception then exists only in the background Future while clients keep
seeing RUNNING. The manager records the failure before its existing terminal cleanup
path seals issuance, cancels outstanding tasks, verifies exact drain receipts, and
releases ownership. Typed Pangrova errors retain their stable code in asynchronous
query status, including authorization failures after a Native commit.

The lifecycle review also checked [Trino's failure transition](https://github.com/trinodb/trino/blob/master/core/trino-main/src/main/java/io/trino/execution/QueryStateMachine.java):
it retains the original failure before notifying state listeners and protects failure
publication from cleanup exceptions. Pangrova additionally requires its metadata
admission to remain open until the terminal transition; its physical cleanup order
must respect that local invariant rather than copying another engine's call order.

### 4. Slow-Node Tolerance

Can a slow node block the entire operation indefinitely? Worker heartbeat timeout (default 60s) prevents false positives. Circuit breaker on metadata store after 5 consecutive slow responses.

### CI Enforcement

The `distributed-checklist` CI job detects critical module changes, verifies the PR body contains a "Distributed Correctness" section with at least one checked item and explanation.

---
title: "Resilience"
description: "Error taxonomy, circuit breakers, degradation, retries, and client error mapping"
sidebar_label: "Resilience"
---

# Pangrova — Resilience

> **Accuracy scope.** This is a living architecture document. Content explicitly marked
> draft, proposed, planned, phase, target, or remaining work is design intent rather
> than implemented support. For current behavior, code wiring and end-to-end tests
> take precedence.

> **Summary.** The error-code taxonomy, circuit breakers, graceful degradation, and retry policy.

## 1. Resilience Architecture Overview

![Failure handling and recovery: classify the failure, contain it with circuit breakers and retry budgets, recover or degrade, and report the exact client outcome](/img/diagrams/resilience-flow.svg)

---

## 2. Unified Error-Code System

> **Implementation note**: The production `PangrovaErrorCode` enum uses string-prefix codes
> (`CATEGORY-NNNN` format, e.g., `PARSER-0001`, `SECURITY-0001`, `NATIVE-0001`) — not the
> integer-range format shown in the code snippets below. The category grouping and retry
> semantics remain accurate; the code format has evolved from integer ranges to string prefixes.
> See `pangrova-common/.../exception/PangrovaErrorCode.java` for the authoritative enum.

### 2.1 Error Classification

`PangrovaErrorCode` values use the string format `CATEGORY-NNNN` (for example
`PARSER-0001`, `RESOURCE-0001`, `NATIVE-0001`). `ErrorClassifier` maps the prefix
before the first hyphen to an `ErrorCategory`; unknown prefixes default to
`INTERNAL_ERROR`.

| Prefixes | ErrorCategory | Retryable | Max retries |
|---|---|---|---|
| `USER`, `VALIDATION`, `TENANT` | `USER_ERROR` | No | 0 |
| `RESOURCE`, `MEMORY` | `RESOURCE_ERROR` | Yes | 1 |
| `INTERNAL`, `CONFIG`, unknown | `INTERNAL_ERROR` | No | 0 |
| `EXTERNAL`, `METADATA`, `NATIVE` | `EXTERNAL_ERROR` | Yes | 3 |
| `EXCHANGE`, `FLIGHT`, `TRANSPORT` | `EXCHANGE_ERROR` | Yes | 3 |
| `TIMEOUT` | `TIMEOUT` | Yes | 2 |

Prefixes outside this map (for example `PARSER`, `PLANNER`, `SECURITY`, `AI`) are
currently classified as `INTERNAL_ERROR`. That is the current `ErrorClassifier`
behavior, not a claim that every prefix is semantically ideal.

### 2.2 Error-Code Definition

`PangrovaErrorCode` is an enum of string codes and descriptions:

```java
PARSER_SYNTAX_ERROR("PARSER-0001", "SQL syntax error"),
PLANNER_VALIDATION_FAILURE("PLANNER-0001", "Query validation failure"),
RESOURCE_MEMORY_EXCEEDED("RESOURCE-0001", "Memory quota exceeded"),
RUNTIME_QUERY_TIMEOUT("RUNTIME-0003", "Query execution timeout"),
NATIVE_JNI_EXCEPTION("NATIVE-0001", "C++ exception during JNI call"),
METADATA_TABLE_NOT_FOUND("METADATA-0008", "Table not found"),
```

`getCode()` returns the string code and `getDescription()` returns the description.
There is no numeric code field and no `isRetryable()` method on the enum;
retryability is derived by `ErrorClassifier` from the category prefix.

### 2.3 Exception Class Hierarchy

```java
public class PangrovaException extends RuntimeException {
    private final PangrovaErrorCode errorCode;

    public PangrovaException(PangrovaErrorCode errorCode, String message) { ... }
    public PangrovaException(PangrovaErrorCode errorCode, String message, Throwable cause) { ... }
    public PangrovaErrorCode getErrorCode() { ... }
}
```

The base exception carries only the error code and message/cause. There is no
context map, trace ID, or `toErrorResponse()` method on the base class. Domain
subclasses add their own context (for example `NativeExecutionException` retains
the raw native code). REST responses are produced by `CoordinatorExceptionMapper`,
not by the exception itself.

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

Current implementation is not a hundreds-bucket mapper:

- A negative JNI return code raises `NativeExecutionException`, which uses
  `PangrovaErrorCode.NATIVE_JNI_EXCEPTION` and retains the raw `nativeErrorCode()`.
- Canonical task reports carry an `ExecutionError` with `error_class`, `code`,
  `message`, `retryable`, `failed_component`, and `failed_partition_ids`.
- `TaskCompletionMonitor` surfaces the code, class, and message to query failure
  handling.
- `ErrorClassifier` classifies Java `PangrovaErrorCode` prefixes only; it does not
  map native integer buckets.

### 3.4 HTTP Status Mapping

`CoordinatorExceptionMapper` resolves the HTTP status and writes an `ErrorDetail`
JSON body. It does not derive status from numeric error-code ranges.

| Exception | HTTP status | Error code |
|---|---|---|
| `WebApplicationException` | its response status | retained or status token |
| `IllegalArgumentException` | 400 | `BAD_REQUEST` |
| `AuthenticationRequiredException` | 401 | `AUTHENTICATION_REQUIRED` |
| `AccessDeniedException` / `ForbiddenException` | 403 | `ACCESS_DENIED` |
| not-found exceptions; `METADATA_FILE_SPACE_NOT_FOUND`; `FILE_OBJECT_NOT_FOUND` | 404 | actual Pangrova code |
| `FILE_PATH_INVALID` | 400 | actual Pangrova code |
| `UnregisteredWorkerException` | 409 | actual Pangrova code |
| `TenantQuotaExceededException` | 429 | actual Pangrova code or `RATE_LIMITED` |
| `NoAvailableWorkerException`; `FILE_STORAGE_UNAVAILABLE`; `FILE_STORAGE_NOT_CONFIGURED` | 503 | actual Pangrova code or `SERVICE_UNAVAILABLE` |
| other exceptions | 500 | `INTERNAL_ERROR` |

`error_type` is `USER_ERROR` for statuses from 400 through 499 and
`INTERNAL_ERROR` otherwise. A `PangrovaException` supplies its
`getErrorCode().getCode()` string; JAX-RS protocol statuses retain status tokens
such as `METHOD_NOT_ALLOWED`, `NOT_ACCEPTABLE`, `PAYLOAD_TOO_LARGE`, and
`UNSUPPORTED_MEDIA_TYPE`.

### 3.5 Arrow Flight CallStatus Mapping

`BatchStreamProducer.wireFailure` maps a `DataProducerException.Status` to an
Arrow `CallStatus` for the result stream:

| DataProducer status | Arrow CallStatus |
|---|---|
| `NOT_FOUND` | `NOT_FOUND` |
| `INVALID_ARGUMENT` | `INVALID_ARGUMENT` |
| `RESOURCE_EXHAUSTED` | `RESOURCE_EXHAUSTED` |
| `UNAVAILABLE` | `UNAVAILABLE` |
| `CANCELLED` | `CANCELLED` |
| `OK`, `INTERNAL` | `INTERNAL` |

An existing `FlightRuntimeException` is rethrown unchanged. Producer timeout and
interruption paths use `TIMED_OUT` and `CANCELLED` directly. This is a transport
mapping, not a second error-code taxonomy.

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

Retry eligibility comes from `ErrorClassifier` categories, not a hard-coded set of
enum values:

```java
ErrorClassifier classifier = new ErrorClassifier();
ErrorCategory category = classifier.classify(error.getErrorCode());
boolean retryable = classifier.isRetryable(category)
        && attempt < classifier.maxRetries(category);

RetryPolicy policy = new RetryPolicy(); // 100 ms -> 10 s, x2, +/-50% jitter
Duration backoff = policy.getBackoff(attempt);
```

`RetryPolicy.shouldRetry(category, attempt, idempotent)` also refuses TIMEOUT retries
for non-idempotent operations. Max retries by category are defined in
`ErrorClassifier`: USER/INTERNAL 0, RESOURCE 1, TIMEOUT 2, EXTERNAL/EXCHANGE 3.

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

`ErrorDetail` is the actual REST error body:

```json
{
  "error_code": "METADATA-0008",
  "error_type": "USER_ERROR",
  "message": "Table 'iceberg.db.orders' not found"
}
```

`stack_trace` is optional and omitted when null. There is no `traceId`, `queryId`,
`context`, `suggestion`, or `documentationUrl` field in `ErrorDetail`.

### 8.2 Error Suggestion Generation

There is no server-side suggestion generator in the current code. Clients may
map `error_code` values to local guidance; the server does not attach suggestions.

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
        if (new ErrorClassifier().classify(e.getErrorCode())
                == ErrorClassifier.ErrorCategory.INTERNAL_ERROR) {
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



## 12. Stage and task retry

A live query owns a DAG and its task-terminal registrations. Each registered task is
bound to its Stage instance and attempt generation. The authenticated report receiver
first validates the issued request and Worker identity. A report may arrive before
the dispatch response reaches the Coordinator. The DAG buffers the first terminal
fact and replays it once after the whole cohort is known. Duplicate reports preserve
the original outcome. Retirement closes admission and clears pending and delivered
registrations. Listeners run outside the registry and Stage accounting locks.

`Stage` owns the attempt generation and retry budget. Once its task cohort reaches
FAILED, `RetryStageHandler` classifies the error and applies its Resilience4j
limit to the Stage's own policy. A retryable source stage can restart as a whole
while downstream stages remain planned. A Worker-loss replacement for a single
exchange-free task inherits that logical task's retry budget. A dispatch failure
refunds an unused reservation and replays the original terminal report if it
arrived during the swap. A successful retry binds the selected Worker incarnation
and the new control/input seals before transport entry. An ambiguous transport
failure keeps the issued identity and cancels it on that Worker until real drain.

The client-facing OUTPUT endpoint pins the issued attempt when the Coordinator
hands it to the result sink. Once published, a live Flight reader cannot switch
to a different attempt. Existing DML commit fragments also pin write attempts.
Each admitted downstream consumer pins its producer generation before binding
exchange records, including the interval before the consumer starts scheduling.
DAG terminal signals drive logical query completion. Physical attempts remain
tracked for drain; a replaced failed task cannot fail a recovered query. A
non-DAG query still uses the task tracker for completion. Query cancellation
resolves pending DAG observers before waiting for physical cleanup. Cancellation
and unclassified stage errors preserve failure.

Query cancellation, stage dispatch and speculative attempts share a Coordinator
queue for exact-attempt seals. The task timeout sweep retries the queue even
when its timeout set is empty. An independently received and validated exit
receipt retires the corresponding retry after request compaction.

An OUTPUT task can report FINISHED before its result memory is released.
If its query has retired, the Coordinator may already hold the actual Native
release receipt from a concurrent seal. For that exact read-result attempt,
the control response `RETIRED_RESULT_NOTICE` lets the Worker stop resending
the older undrained notification. The subsequent physical-release report still
requires its own matching `CONTINUE`. The control response performs no output
publication, write commit, or query state transition.

A replacement consumer would need to reacquire read authority for its exact
published partitions. Direct tickets and external read grants are bound to the
original request. Current providers do not advertise consumer replay for a new
attempt. Both task replacement and whole-stage retry decline affected consumers.
Moving a single producer also requires replacing its downstream placement record
and producer authority. These requests fail admission before a second dispatch.
The current runtime does not cascade retries into previously finished producers.

## 13. Lineage-Based Recovery Mode (OPT-017)

> Implementation status (2026-11-02): the previously unassembled FteStageHandler (retired) /
> FteAwareTaskFaultHandler (retired) and their in-place upstream recomputation helper have been
> retired. They did not provide a production recovery path and could report success
> after the state machine rejected reopening FINISHED output. The active DAG chain uses
> `RetryStageHandler`; its bounded failed-stage retry is not proof of upstream output
> regeneration. The lineage/RECOMPUTE design below remains a proposal requiring a new
> exchange generation, explicit committed-attempt selection and verified resource drain.
> Do not enable or advertise it from the presence of policy/scheduler model classes.

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

## 15. Durable process incarnation ownership

`ProcessIncarnationStore` supplies a local physical-exit proof for services that
retain task admission in memory. A persistent directory holds an opaque domain
UUID and a checksum-chained incarnation ledger. Startup takes an exclusive file
lock, validates every complete ledger record, and forces the new incarnation to
disk before exposing its identity. Only a successor holding that same lock can
recognize recorded predecessor incarnations as retired. A different directory,
unknown incarnation, expired lease, or missing heartbeat does not establish exit.

The lock is held until the owning service actually joins all physical execution
owners, or the JVM terminates. `closeAfterDrain` deliberately requires the real
join callback rather than implementing generic `AutoCloseable`. Failed joins
retain the lock. Beginning shutdown permanently closes admission of new named
authorities, even if the join fails; an authority cannot be recreated under the
same incarnation after losing its in-memory admission state. Drain callbacks can
still inspect the identity and retired incarnations.

Use a persistent local filesystem supporting exclusive file locks. Keep each
process domain in its own directory; never copy, replace, or delete that directory
while it is serving execution ownership. Java documents that file locks are held
on behalf of the JVM and released on channel closure or JVM termination, so an
application-level join must precede explicit release. See the
[JDK 17 FileLock contract](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/nio/channels/FileLock.html).
The append-before-admission ordering follows the same durability principle as
[PostgreSQL write-ahead logging](https://www.postgresql.org/docs/16/wal-intro.html).
These primitives provide proof only when the service binds requests to the exact
identity and enforces admission and shutdown ownership; they do not by themselves
complete Worker or Coordinator recovery wiring.

Local checks cover a killed independent JVM, successful and failed joins,
concurrent shutdown, rejection of late/recreated authorities, unknown identities,
a torn unpublished startup tail, and corruption of a complete ledger record.

## Appendix: Distributed Correctness PR Checklist

CI enforces this checklist when PRs modify `pangrova-coordination/pangrova-coordination-control/`, `pangrova-worker/`, `pangrova-shuffle/`, or `pangrova-metadata/`.

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

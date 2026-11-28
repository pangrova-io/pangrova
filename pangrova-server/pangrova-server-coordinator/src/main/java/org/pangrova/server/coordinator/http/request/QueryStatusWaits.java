package org.pangrova.server.coordinator.http.request;

import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.container.CompletionCallback;
import jakarta.ws.rs.container.ConnectionCallback;
import jakarta.ws.rs.core.Response;
import org.pangrova.common.client.error.ErrorDetail;
import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.common.enums.QueryWaitTarget;
import org.pangrova.observability.api.metrics.DoubleHistogram;
import org.pangrova.observability.api.metrics.LongCounter;
import org.pangrova.observability.api.metrics.LongUpDownCounter;
import org.pangrova.observability.api.metrics.MetricAttributes;
import org.pangrova.observability.api.metrics.MetricsProvider;
import org.pangrova.query.lifecycle.QueryStateMachine;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Predicate;

/** Bounds suspended status HTTP requests and resumes them on the server's executor. */
@ThreadSafe
public final class QueryStatusWaits {
    private static final int MAX_WAITERS = 4096;
    private static final int MAX_WAITERS_PER_TENANT = 1024;
    private static final int MAX_WAITERS_PER_PRINCIPAL = 256;
    private static final int HTTP_TOO_MANY_REQUESTS = 429;

    private final Executor executor;
    private final WaitMetrics metrics;
    private final Set<Observer> active = new HashSet<>();
    private final Map<String, Integer> tenantCounts = new HashMap<>();
    private final Map<String, Integer> principalCounts = new HashMap<>();

    public QueryStatusWaits(Executor executor, MetricsProvider metricsProvider) {
        this.executor = Objects.requireNonNull(executor, "executor");
        this.metrics = metricsProvider == null ? null : new WaitMetrics(metricsProvider);
    }

    /** Admits one observer and detaches it when the HTTP exchange ends. */
    public void suspend(String queryId, String principal, String observerId, QueryStateMachine query,
                        QueryWaitTarget target, long version, long waitMillis, AsyncResponse response,
                        Function<QueryStateMachine.Status, Response> render) {
        var observer = new Observer(queryId, principal, observerId);
        String tenant = query.getTenantId();
        Response refusal = admit(observer, tenant);
        if (refusal != null) {
            if (metrics != null) {
                metrics.completed(target, refusal.getStatus() == HTTP_TOO_MANY_REQUESTS
                        ? Outcome.LIMIT : Outcome.DUPLICATE, 0);
            }
            response.resume(refusal);
            return;
        }
        long started = metrics == null ? 0 : System.nanoTime();
        if (metrics != null) metrics.active().add(1);
        CompletableFuture<QueryStateMachine.Status> update;
        try {
            update = query.awaitStatus(target, version);
        } catch (RuntimeException failure) {
            finish(observer, tenant, target, Outcome.FAILED, started);
            throw failure;
        }
        var detached = new AtomicBoolean();
        Predicate<Outcome> detach = outcome -> {
            if (!detached.compareAndSet(false, true)) return false;
            update.cancel(false);
            finish(observer, tenant, target, outcome, started);
            return true;
        };
        try {
            response.register(
                    (CompletionCallback) error -> detach.test(error == null ? Outcome.CLOSED : Outcome.FAILED),
                    (ConnectionCallback) disconnected -> detach.test(Outcome.DISCONNECTED));
            response.setTimeoutHandler(expired -> {
                if (detach.test(Outcome.TIMEOUT)) resume(expired, render, query.status());
            });
            if (!response.setTimeout(waitMillis, TimeUnit.MILLISECONDS)) {
                detach.test(Outcome.CLOSED);
                return;
            }
            update.thenAccept(status -> {
                try {
                    executor.execute(() -> {
                        if (detach.test(Outcome.CHANGED)) resume(response, render, status);
                    });
                } catch (RejectedExecutionException stopping) {
                    if (detach.test(Outcome.EXECUTOR_REJECTED)) {
                        response.resume(Response.status(Response.Status.SERVICE_UNAVAILABLE)
                                .entity(new ErrorDetail("QUERY_STATUS_WAIT_UNAVAILABLE", null,
                                        "Query status wait executor is unavailable"))
                                .build());
                    }
                }
            });
        } catch (RuntimeException failure) {
            detach.test(Outcome.FAILED);
            throw failure;
        }
    }

    private static void resume(AsyncResponse response, Function<QueryStateMachine.Status, Response> render,
                               QueryStateMachine.Status status) {
        Response rendered;
        try {
            rendered = render.apply(status);
        } catch (RuntimeException failure) {
            response.resume(failure);
            return;
        }
        response.resume(rendered);
    }

    private void finish(Observer observer, String tenant, QueryWaitTarget target, Outcome outcome, long started) {
        release(observer, tenant);
        if (metrics != null) {
            metrics.active().add(-1);
            metrics.completed(target, outcome, System.nanoTime() - started);
        }
    }

    private synchronized Response admit(Observer observer, String tenant) {
        if (active.contains(observer)) {
            return Response.status(Response.Status.CONFLICT)
                    .entity(new ErrorDetail("QUERY_STATUS_WAIT_EXISTS", null, "Observer already has a pending wait"))
                    .build();
        }
        String principal = observer.principal();
        if (active.size() >= MAX_WAITERS
                || tenantCounts.getOrDefault(tenant, 0) >= MAX_WAITERS_PER_TENANT
                || principalCounts.getOrDefault(principal, 0) >= MAX_WAITERS_PER_PRINCIPAL) {
            return Response.status(HTTP_TOO_MANY_REQUESTS)
                    .entity(new ErrorDetail("QUERY_STATUS_WAIT_LIMIT", null, "Query status wait capacity is exhausted"))
                    .build();
        }
        active.add(observer);
        tenantCounts.merge(tenant, 1, Integer::sum);
        principalCounts.merge(principal, 1, Integer::sum);
        return null;
    }

    private synchronized void release(Observer observer, String tenant) {
        if (!active.remove(observer)) return;
        tenantCounts.computeIfPresent(tenant, (key, count) -> count == 1 ? null : count - 1);
        principalCounts.computeIfPresent(observer.principal(), (key, count) -> count == 1 ? null : count - 1);
    }

    private record Observer(String queryId, String principal, String observerId) {}

    private enum Outcome { CHANGED, TIMEOUT, DISCONNECTED, DUPLICATE, LIMIT, CLOSED, FAILED, EXECUTOR_REJECTED }

    private record WaitMetrics(LongUpDownCounter active, LongCounter completions, DoubleHistogram duration) {
        private static final double NANOS_PER_MILLISECOND = 1_000_000.0;
        private static final List<Double> WAIT_DURATION_BUCKETS_MILLIS =
                List.of(1.0, 10.0, 50.0, 100.0, 500.0, 1_000.0, 5_000.0, 30_000.0);

        WaitMetrics(MetricsProvider provider) {
            this(provider.createUpDownCounter(
                        "pangrova.query.status_wait.active", "Suspended query status observers", "requests"),
                    provider.createCounter(
                            "pangrova.query.status_wait.completed", "Query status wait outcomes", "requests"),
                    provider.createHistogram("pangrova.query.status_wait.duration",
                            "Time a query status observer held capacity", "ms", WAIT_DURATION_BUCKETS_MILLIS));
        }

        void completed(QueryWaitTarget target, Outcome outcome, long elapsedNanos) {
            var attributes = MetricAttributes.of("target", target.name(), "outcome", outcome.name());
            completions.add(1, attributes);
            if (outcome != Outcome.LIMIT && outcome != Outcome.DUPLICATE) {
                duration.record(elapsedNanos / NANOS_PER_MILLISECOND, attributes);
            }
        }
    }
}

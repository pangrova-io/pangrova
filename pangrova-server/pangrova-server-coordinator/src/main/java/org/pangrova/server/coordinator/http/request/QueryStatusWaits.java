package org.pangrova.server.coordinator.http.request;

import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.container.CompletionCallback;
import jakarta.ws.rs.container.ConnectionCallback;
import jakarta.ws.rs.core.Response;
import org.pangrova.common.client.error.ErrorDetail;
import org.pangrova.common.client.query.QueryWaitTarget;
import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.query.lifecycle.QueryStateMachine;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

/** Bounds suspended status HTTP requests and resumes them on the server's executor. */
@ThreadSafe
public final class QueryStatusWaits {
    private static final int MAX_WAITERS = 4096;
    private static final int MAX_WAITERS_PER_TENANT = 1024;
    private static final int MAX_WAITERS_PER_PRINCIPAL = 256;
    private static final int HTTP_TOO_MANY_REQUESTS = 429;

    private final Executor executor;
    private final Set<Observer> active = new HashSet<>();
    private final Map<String, Integer> tenantCounts = new HashMap<>();
    private final Map<String, Integer> principalCounts = new HashMap<>();

    public QueryStatusWaits(Executor executor) {
        this.executor = java.util.Objects.requireNonNull(executor, "executor");
    }

    /** Admits one observer and detaches it when the HTTP exchange ends. */
    public void suspend(String queryId, String principal, String observerId, QueryStateMachine query,
                        QueryWaitTarget target, long version, long waitMillis, AsyncResponse response,
                        Function<QueryStateMachine.Status, Response> render) {
        var observer = new Observer(queryId, principal, observerId);
        String tenant = query.getTenantId();
        Response refusal = admit(observer, tenant);
        if (refusal != null) {
            response.resume(refusal);
            return;
        }
        var update = query.awaitStatus(target, version);
        var detached = new AtomicBoolean();
        Runnable detach = () -> {
            if (detached.compareAndSet(false, true)) {
                update.cancel(false);
                release(observer, tenant);
            }
        };
        try {
            response.register((CompletionCallback) error -> detach.run(),
                    (ConnectionCallback) disconnected -> detach.run());
            response.setTimeoutHandler(expired -> {
                detach.run();
                expired.resume(render.apply(query.status()));
            });
            if (!response.setTimeout(waitMillis, TimeUnit.MILLISECONDS)) {
                detach.run();
                return;
            }
            update.thenAccept(status -> {
                try {
                    executor.execute(() -> {
                        detach.run();
                        response.resume(render.apply(status));
                    });
                } catch (RejectedExecutionException stopping) {
                    detach.run();
                    // The container owns the remaining response timeout during shutdown.
                }
            });
        } catch (RuntimeException failure) {
            detach.run();
            throw failure;
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
}

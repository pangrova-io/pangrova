package org.pangrova.server.coordinator.http.endpoint;

import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.core.Response;
import java.lang.reflect.Proxy;
import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.testinfra.StatusOnlyQueryManagerFixture;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.common.client.query.SubmitQueryRequest;
import org.pangrova.common.client.query.SubmitQueryResponse;
import org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory;
import org.pangrova.persistence.coordination.memory.membership.InMemoryWorkerRegistry;
import org.pangrova.persistence.query.memory.state.InMemoryQueryStateJournal;
import org.pangrova.query.api.exception.IdempotencyConflictException;
import org.pangrova.query.runtime.QueryManager;
import org.pangrova.security.api.exception.AuthenticationRequiredException;
import org.pangrova.security.api.identity.UserIdentity;
import org.pangrova.server.coordinator.http.request.QueryStatusWaits;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class QueryResourceIdempotencyTest {
    @Test
    void replayUsesTheOriginalReceiptAndPrincipalsHaveIndependentKeySpaces() {
        var journal = new InMemoryQueryStateJournal();
        var queries = StatusOnlyQueryManagerFixture.create(
                new WorkerManager(20_000, 60_000, new InMemoryWorkerRegistry()),
                new CoordinatorTaskTracker(), journal);
        var resource = new QueryResource(queries, null, null,
                new OtelExecutionTraceContextFactory(), new QueryStatusWaits(Runnable::run), () -> null, null);
        try {
            var request = new SubmitQueryRequest("SELECT 1", Map.of());
            SubmitQueryResponse original = submit(resource, "alice", "key-a", request);
            SubmitQueryResponse replay = submit(resource, "alice", "key-a", request);
            SubmitQueryResponse otherPrincipal = submit(resource, "bob", "key-a", request);

            assertThat(original.queryId()).isNotBlank();
            assertThat(replay).isEqualTo(original);
            assertThat(otherPrincipal.queryId()).isNotEqualTo(original.queryId());
            SubmitQueryResponse fresh = submit(resource, "alice", null, request);
            assertThat(fresh.state()).isEqualTo("QUEUED");
            assertThat(fresh.resultDisposition()).isEqualTo(org.pangrova.common.enums.ResultDisposition.PENDING);
            assertThat(submit(resource, "alice", null, request).queryId()).isNotEqualTo(fresh.queryId());
            assertThat(submit(resource, "alice", fresh.queryId(), request).queryId()).isNotEqualTo(fresh.queryId());
            assertThatThrownBy(() -> submit(resource, "alice", "key-a",
                    new SubmitQueryRequest("SELECT 2", Map.of())))
                    .isInstanceOf(IdempotencyConflictException.class);
            assertThatThrownBy(() -> submit(resource, null, "key-a", request))
                    .isInstanceOf(AuthenticationRequiredException.class);
            assertThatThrownBy(() -> submit(resource, null, null, request))
                    .isInstanceOf(AuthenticationRequiredException.class);
        } finally {
            journal.close();
        }
    }

    private static SubmitQueryResponse submit(QueryResource resource, String principal,
            String key, SubmitQueryRequest request) {
        var identity = principal == null ? null
                : new UserIdentity(principal, Set.of(), Map.of(), Instant.EPOCH);
        var resumed = new AtomicReference<>();
        ContainerRequestContext context = (ContainerRequestContext) Proxy.newProxyInstance(
                ContainerRequestContext.class.getClassLoader(), new Class<?>[] {ContainerRequestContext.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("getProperty")) {
                        return AuthenticationFilter.USER_IDENTITY_PROPERTY.equals(args[0]) ? identity : null;
                    }
                    if (method.getName().equals("getHeaderString")) {
                        return "Idempotency-Key".equals(args[0]) ? key : null;
                    }
                    throw new UnsupportedOperationException(method.getName());
                });
        AsyncResponse response = (AsyncResponse) Proxy.newProxyInstance(
                AsyncResponse.class.getClassLoader(), new Class<?>[] {AsyncResponse.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("resume")) {
                        resumed.set(args[0]);
                        return null;
                    }
                    throw new UnsupportedOperationException(method.getName());
                });
        resource.submitQuery(request, context, response);
        return (SubmitQueryResponse) ((Response) resumed.get()).getEntity();
    }
}

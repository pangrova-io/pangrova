package org.pangrova.server.coordinator.http.request;

import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.container.CompletionCallback;
import jakarta.ws.rs.container.ConnectionCallback;
import jakarta.ws.rs.container.TimeoutHandler;
import jakarta.ws.rs.core.Response;
import org.junit.jupiter.api.Test;
import org.pangrova.common.enums.QueryWaitTarget;
import org.pangrova.query.api.enums.QueryState;
import org.pangrova.query.lifecycle.QueryStateMachine;

import java.lang.reflect.Proxy;
import java.util.Map;
import java.util.concurrent.RejectedExecutionException;

import static org.assertj.core.api.Assertions.assertThat;

class QueryStatusWaitsTest {
    @Test
    void boundsWaitsPerPrincipalAndTenantAndReturnsCapacityAfterDisconnect() {
        var waits = new QueryStatusWaits(Runnable::run, null);
        var initial = new PendingWait();
        for (int principal = 0; principal < 4; principal++) {
            var state = query("busy-tenant");
            for (int observer = 0; observer < 256; observer++) {
                var wait = principal == 0 && observer == 0 ? initial : new PendingWait();
                suspend(waits, state, "p-" + principal, "observer-" + observer, wait);
                assertThat(wait.response).isNull();
            }
            assertThat(suspend(waits, state, "p-" + principal, "extra", new PendingWait()).getStatus())
                    .isEqualTo(429);
        }
        var fifthPrincipal = query("busy-tenant");
        assertThat(suspend(waits, fifthPrincipal, "p-4", "new", new PendingWait()).getStatus())
                .isEqualTo(429);
        assertThat(suspend(waits, query("other-tenant"), "independent", "new", new PendingWait())).isNull();

        initial.disconnect();
        initial.expire();
        assertThat(initial.response).isNull();
        assertThat(suspend(waits, fifthPrincipal, "p-4", "new", new PendingWait())).isNull();
        assertThat(suspend(waits, fifthPrincipal, "p-4", "second", new PendingWait()).getStatus())
                .isEqualTo(429);
    }

    @Test
    void rejectedResponseExecutorReturnsAServiceErrorAndDetachesTheObserver() {
        var waits = new QueryStatusWaits(command -> {
            throw new RejectedExecutionException("response executor stopped");
        }, null);
        var state = query("tenant");
        var wait = new PendingWait();
        assertThat(suspend(waits, state, "principal", "observer", wait)).isNull();
        assertThat(state.tryTransition(QueryState.PLANNING)).isTrue();
        assertThat(wait.response.getStatus()).isEqualTo(503);
        wait.expire();
        assertThat(wait.resumes).isOne();
        assertThat(suspend(waits, state, "principal", "observer", new PendingWait())).isNull();
    }

    private static Response suspend(QueryStatusWaits waits, QueryStateMachine state, String principal,
            String observer, PendingWait wait) {
        waits.suspend(principal + "-query", principal, observer, state, QueryWaitTarget.NEXT_VERSION,
                state.status().version(), 5_000, wait.transport, ignored -> Response.ok().build());
        return wait.response;
    }

    private static QueryStateMachine query(String tenant) {
        var state = new QueryStateMachine();
        state.setTenantId(tenant);
        return state;
    }

    /** Runs the container callbacks explicitly without opening thousands of sockets. */
    private static final class PendingWait {
        private CompletionCallback completion;
        private ConnectionCallback connection;
        private TimeoutHandler timeout;
        private Response response;
        private int resumes;
        private final AsyncResponse transport = (AsyncResponse) Proxy.newProxyInstance(
                AsyncResponse.class.getClassLoader(), new Class<?>[] {AsyncResponse.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "register" -> {
                        completion = (CompletionCallback) arguments[0];
                        connection = (ConnectionCallback) ((Object[]) arguments[1])[0];
                        yield Map.of();
                    }
                    case "setTimeoutHandler" -> {
                        timeout = (TimeoutHandler) arguments[0];
                        yield null;
                    }
                    case "setTimeout" -> true;
                    case "resume" -> {
                        response = (Response) arguments[0];
                        resumes++;
                        if (completion != null) completion.onComplete(null);
                        yield true;
                    }
                    default -> throw new UnsupportedOperationException(method.getName());
                });

        private void disconnect() {
            connection.onDisconnect(transport);
            completion.onComplete(null);
        }

        private void expire() {
            timeout.handleTimeout(transport);
        }
    }
}

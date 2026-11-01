package org.pangrova.coordinator.http;

import static org.assertj.core.api.Assertions.assertThat;

import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.pangrova.coordinator.query.execution.QueryDispatcher;
import org.pangrova.coordinator.query.execution.QueryDispatcherContext;
import org.pangrova.coordinator.query.task.TaskStatusReceiver;
import org.pangrova.coordinator.query.task.TaskStatusReceiverContext;
import org.pangrova.coordinator.query.QueryManager;
import org.pangrova.coordinator.query.QueryManagerContext;
import org.pangrova.coordinator.scheduler.RemoteTaskDispatcher;
import org.pangrova.scheduler.core.placement.TaskScheduler;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.testinfra.SignedOidcIssuer;
import org.pangrova.coordinator.testinfra.SourceFreeQueryFixture;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.scheduler.core.scoring.WorkerPerformanceScorer;
import org.pangrova.observability.sdk.noop.NoOpTracingProvider;
import org.pangrova.planner.SqlCompiler;
import org.pangrova.security.core.authn.AuthenticationManager;
import org.pangrova.spi.security.authn.AuthenticationRequest;
import org.pangrova.security.core.authn.provider.oauth2.OAuth2Authenticator;
import org.pangrova.coordination.inmemory.InMemoryWorkerRegistry;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.Map;

/**
 * Regression test for the internal-auth shared secret being omitted from
 * wired into the coordinator {@code AuthenticationFilter}, so with user OAuth2/LDAP auth enabled,
 * worker→coordinator internal calls (heartbeat/registration/task status) carrying the internal
 * secret were wrongly rejected with HTTP 401.
 *
 * <p>This test starts a real {@link CoordinatorHttpServer} with a production OAuth2 authenticator,
 * real signed OIDC discovery/JWKS authority, and an internal secret
 * configured via {@link CoordinatorHttpServerContext#internalAuthSecret()}. It asserts that an
 * internal request carrying the secret is accepted (NOT 401), while external requests without the
 * secret are still rejected.</p>
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class InternalAuthSecretWiringTest {

    private static final String SECRET = "super-internal-secret";
    private static final String HEARTBEAT = "{\"worker_id\":\"unregistered-auth-worker\"}";

    private CoordinatorHttpServer httpServer;
    private SignedOidcIssuer issuer;
    private OAuth2Authenticator authenticator;
    private AuthenticationManager authManager;
    private RemoteTaskDispatcher transport;
    private HttpClient httpClient;
    private String baseUrl;

    @BeforeAll
    void setUp() throws Exception {
        try {
            issuer = new SignedOidcIssuer();
            authenticator = new OAuth2Authenticator();
            authenticator.init(Map.of("issuerUri", issuer.uri(), "audience", "internal-auth-tests"));
            authManager = new AuthenticationManager(List.of(authenticator));
            var tracing = new NoOpTracingProvider();
            transport = new RemoteTaskDispatcher(CircuitBreakerPolicy.noOp(), tracing, SECRET);
            var taskTracker = new CoordinatorTaskTracker();
            var workerManager = new WorkerManager(20_000, 60_000, new InMemoryWorkerRegistry());
            var dispatcherContext = QueryDispatcherContext.withDefaults(new SqlCompiler(), null, null, null,
                    transport, new TaskScheduler(new WorkerPerformanceScorer()), workerManager, taskTracker,
                    null, null, null, 0L);
            var dispatcher = new QueryDispatcher(SourceFreeQueryFixture.withPolicies(dispatcherContext), null);
            var queryManager = new QueryManager(QueryManagerContext.minimal(dispatcher, tracing));
            var receiverContext = TaskStatusReceiverContext.withoutSpeculation(
                    queryManager, taskTracker, workerManager);
            var receiver = new TaskStatusReceiver(receiverContext);
            var serverContext = new CoordinatorHttpServerContext(
                    receiver, queryManager, null, authManager, null, workerManager,
                    null, null, null, null, null, SECRET);
            httpServer = new CoordinatorHttpServer(0, serverContext);
            httpServer.start();
            baseUrl = "http://127.0.0.1:" + httpServer.getPort();
            httpClient = HttpClient.newHttpClient();
        } catch (Exception | Error failure) {
            try {
                tearDown();
            } catch (Exception cleanup) {
                failure.addSuppressed(cleanup);
            }
            throw failure;
        }
    }

    @AfterAll
    void tearDown() throws Exception {
        Exception failure = null;
        AutoCloseable authOwner = authManager == null ? authenticator : authManager;
        for (var owner : new AutoCloseable[]{httpServer == null ? null : httpServer::stop,
                transport, authOwner, issuer}) {
            if (owner == null) {
                continue;
            }
            try {
                owner.close();
            } catch (Exception cleanup) {
                if (failure == null) {
                    failure = cleanup;
                } else {
                    failure.addSuppressed(cleanup);
                }
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    @Test
    void internalRequestWithSecretIsNotRejectedWhenOAuthEnabled() throws Exception {
        // Worker heartbeat is a real internal endpoint registered by WorkerResource.
        HttpResponse<String> response = post("/v1/worker/heartbeat", HEARTBEAT, "Bearer " + SECRET);

        // The internal secret must short-circuit the OAuth2/LDAP filter. Before the fix the
        // filter never received the secret, so this returned 401.
        assertThat(response.statusCode())
                .as("internal request carrying the secret must bypass OAuth2/LDAP auth")
                .isNotEqualTo(401);
        assertThat(response.statusCode()).isEqualTo(409);
        assertThat(response.body()).contains("re-register", "unregistered-auth-worker");
    }

    @Test
    void externalRequestWithoutSecretIsStillRejected() throws Exception {
        HttpResponse<String> response = post("/v1/worker/heartbeat", HEARTBEAT, "Bearer not-the-secret");

        assertThat(response.statusCode())
                .as("external requests without the internal secret must still be 401 under OAuth2/LDAP")
                .isEqualTo(401);
    }

    @Test
    void externalRequestWithNoAuthHeaderIsStillRejected() throws Exception {
        HttpResponse<String> response = post("/v1/worker/heartbeat", HEARTBEAT, null);

        assertThat(response.statusCode()).isEqualTo(401);
    }

    @Test
    void actualSignedExternalIdentityCannotActAsWorker() throws Exception {
        String token = issuer.token("internal-auth-tests", true);
        var identity = authManager.authenticate(new AuthenticationRequest(
                null, null, token, null, Map.of(), "127.0.0.1"));
        assertThat(identity.success()).isTrue();
        assertThat(identity.userIdentity().principal()).isEqualTo("alice");
        var response = post("/v1/worker/heartbeat", HEARTBEAT, "Bearer " + token);
        assertThat(response.statusCode()).isEqualTo(403);
        assertThat(response.body()).contains("ACCESS_DENIED", "USER_ERROR").doesNotContain(SECRET);
    }

    private HttpResponse<String> post(String path, String body, String authHeader)
            throws Exception {
        HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + path))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body));
        if (authHeader != null) {
            builder.header("Authorization", authHeader);
        }
        return httpClient.send(builder.build(), HttpResponse.BodyHandlers.ofString());
    }

}

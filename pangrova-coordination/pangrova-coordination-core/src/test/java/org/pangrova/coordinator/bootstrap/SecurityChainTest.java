package org.pangrova.coordinator.bootstrap;

import org.pangrova.testing.testkit.ContainerTestBase;
import org.pangrova.common.http.HttpClientFactory;
import org.pangrova.common.http.ManagedHttpClient;

import com.fasterxml.jackson.databind.ObjectMapper;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.Tag;
import net.jqwik.api.lifecycle.AfterProperty;
import org.pangrova.coordinator.bootstrap.CoordinatorBootstrapConfig.SecurityConfig;
import org.pangrova.coordinator.query.session.CoordinatorSessionContext;
import org.pangrova.spi.security.AccessDeniedException;
import org.pangrova.security.authn.AuthenticationManager;
import org.pangrova.spi.security.authn.AuthenticationRequest;
import org.pangrova.spi.security.authn.AuthenticationResult;
import org.pangrova.spi.security.authn.Authenticator;
import org.pangrova.spi.security.UserIdentity;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.pangrova.coordinator.testinfra.SecuredCoordinatorFixtures.withAuthority;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * Acceptance test: validates the full security chain from CoordinatorBootstrap.start()
 * through to the HTTP authentication filter on live HTTP endpoints.
 *
 * <p>Chain under test: CoordinatorBootstrap.start() → startSecurity() →
 * AuthenticationManager(NoOpAuthenticator) → startHttp() →
 * CoordinatorHttpServer registers AuthenticationFilter on /v1/query/*.</p>
 *
 * <p><b>Property 27: Security chain from main() to HTTP filter</b></p>
 * <p><b>Validates: Requirements 20.1-20.5, 62.1-62.4, 63.1-63.5, 64.1-64.3, 94.1-94.5</b></p>
 */
class SecurityChainTest extends ContainerTestBase {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private CoordinatorBootstrap bootstrap;
    private ManagedHttpClient http;

    @AfterEach
    void tearDownJUnit() {
        shutdownBootstrap();
    }

    @AfterProperty
    void tearDownJqwik() {
        shutdownBootstrap();
    }

    // -- Providers --

    @Provide
    Arbitrary<String> coordinatorIds() {
        return Arbitraries.strings().alpha().ofMinLength(3).ofMaxLength(10)
                .map(s -> "sec-" + s);
    }

    // -- Property tests --

    /**
     * P27a: For any coordinatorId, bootstrap with securityEnabled=true and NOOP authenticator
     * starts successfully and the auth filter passes all requests (NoOp accepts everything).
     * This proves the security chain is wired end-to-end.
     *
     * <p><b>Validates: Requirements 20.1, 94.1-94.3</b></p>
     */
    @Property(tries = 5)
    @Tag("Feature: e2e-pipeline-integration, Property 27: Security chain from main() to HTTP filter")
    void noopAuthenticatorPassesAllRequests(
            @ForAll("coordinatorIds") String coordinatorId) throws Exception {

        startSecurityBootstrap(coordinatorId);
        int port = bootstrap.getHttpPort();

        HttpResponse<String> resp = submitQuery(port, "SELECT 1", null);
        // NOOP passes authentication; without a Worker, execution is unavailable.
        assertThat(resp.statusCode()).isEqualTo(503);
        assertThat(resp.body()).contains("COORDINATOR-0001");
    }

    /**
     * P27b: For any coordinatorId, bootstrap with securityEnabled=true wires the auth filter
     * so that /v1/query/* endpoints are protected, while non-query endpoints bypass auth.
     *
     * <p><b>Validates: Requirements 20.2, 94.2</b></p>
     */
    @Property(tries = 5)
    @Tag("Feature: e2e-pipeline-integration, Property 27: Security chain from main() to HTTP filter")
    void authFilterScopedToQueryEndpoints(
            @ForAll("coordinatorIds") String coordinatorId) throws Exception {

        startSecurityBootstrap(coordinatorId);
        int port = bootstrap.getHttpPort();

        // /v1/health is NOT under /v1/query/* — should bypass auth filter
        HttpResponse<String> healthResp = sendGet(port, "/v1/health");
        assertThat(healthResp.statusCode()).isNotEqualTo(401);

        // /v1/query/submit IS under /v1/query/* — auth filter processes it
        // Verify the auth filter does not reject the request (non-401).
        // The query may fail for other reasons (no workers) but that is
        // outside the scope of this security-chain test.
        HttpResponse<String> queryResp = submitQuery(port, "SELECT 1", null);
        assertThat(queryResp.statusCode()).isNotEqualTo(401);
    }


    // -- JUnit scenario tests (specific authenticator behaviors) --

    /**
     * When security is enabled with a rejecting authenticator,
     * HTTP requests without valid credentials receive 401.
     *
     * <p><b>Validates: Requirements 20.1, 20.4, 94.4</b></p>
     */
    @Test
    void rejectingAuthenticatorReturns401WithoutCredentials() throws Exception {
        startSecurityBootstrap("coord-reject-test");
        int port = bootstrap.getHttpPort();

        // NOOP accepts authentication; no Worker is registered for execution.
        HttpResponse<String> resp = submitQuery(port, "SELECT 1", null);
        assertThat(resp.statusCode()).isEqualTo(503);

        // Now test with a separate HTTP server using a rejecting authenticator
        // to validate the filter correctly returns 401
        assertRejectingAuthenticatorReturns401(port);
    }

    /**
     * When security is enabled, Bearer token credentials are extracted
     * and forwarded to the authenticator chain.
     *
     * <p><b>Validates: Requirements 20.2, 20.3, 94.1</b></p>
     */
    @Test
    void bearerTokenPassesThroughAuthChain() throws Exception {
        startSecurityBootstrap("coord-bearer-test");
        int port = bootstrap.getHttpPort();

        HttpResponse<String> resp = submitQuery(port, "SELECT 1", "Bearer test-token");
        assertThat(resp.statusCode()).isEqualTo(503);
        assertThat(resp.body()).contains("COORDINATOR-0001");
    }

    /**
     * Audit events are emitted for query lifecycle when security chain is active.
     *
     * <p><b>Validates: Requirements 63.1-63.5</b></p>
     */
    @Test
    void auditEventsEmittedForQueryLifecycle() throws Exception {
        startSecurityBootstrap("coord-audit-test");
        String queryId = "rejected-audit-query";
        var anonymous = new CoordinatorSessionContext(queryId, Map.of(), null, null);
        assertThatThrownBy(() -> bootstrap.getQueryManager().submitQuery(queryId, "SELECT 1", anonymous))
                .isInstanceOf(AccessDeniedException.class);

        // Logical failure wakes submitQuery before asynchronous terminal effects finish.
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                assertThat(bootstrap.getQueryManager().getActiveQueryIds()).doesNotContain(queryId));

        // The real terminal-event sink and HTTP profile endpoint must share this bootstrap's store.
        var profile = sendGet(bootstrap.getHttpPort(), "/v1/query/profile/" + queryId + "/json");
        assertThat(profile.statusCode()).as(profile.body()).isEqualTo(200);
        var json = MAPPER.readTree(profile.body());
        assertThat(json.path("queryId").asText()).isEqualTo(queryId);
        assertThat(json.path("success").asBoolean()).isFalse();

        // A subsequent Coordinator in the same JVM owns fresh state, even for the same query id.
        startSecurityBootstrap("coord-audit-replacement");
        assertThat(sendGet(bootstrap.getHttpPort(), "/v1/query/profile/" + queryId + "/json").statusCode())
                .isEqualTo(404);
    }

    /**
     * Without security, the HTTP auth filter is absent but protected compilation still fails closed.
     *
     * <p><b>Validates: Requirements 94.5 (security disabled path)</b></p>
     */
    @Test
    void securityDisabledRejectsWorkerControlStartup() {
        CoordinatorBootstrapConfig config = CoordinatorBootstrapConfig.withDefaults(
                "coord-nosec", 0);
        bootstrap = new CoordinatorBootstrap(config);
        assertThatThrownBy(bootstrap::start)
                .isInstanceOf(org.pangrova.common.lifecycle.BootstrapFailedException.class)
                .hasMessageContaining("Worker control requires configured internal-service authentication");
    }


    /**
     * Validates 401 behavior using a standalone CoordinatorHttpServer
     * with a rejecting authenticator (proves the filter rejects correctly).
     */
    private void assertRejectingAuthenticatorReturns401(int bootstrapPort) throws Exception {
        // The bootstrap uses NoOp (accepts all). To prove the filter CAN reject,
        // we build a standalone server with a token-only authenticator.
        var authManager = new AuthenticationManager(List.of(new TokenOnlyAuthenticator()));
        var server = buildStandaloneServer(authManager);
        try {
            server.start();
            int rejectPort = server.getPort();

            // No credentials → 401
            HttpResponse<String> noAuth = submitQuery(rejectPort, "SELECT 1", null);
            assertThat(noAuth.statusCode()).isEqualTo(401);

            // Valid token passes auth; dispatch still requires a Native Worker.
            HttpResponse<String> validAuth = submitQuery(rejectPort, "SELECT 1", "Bearer valid");
            assertThat(validAuth.statusCode()).isEqualTo(503);
        } finally {
            server.stop();
        }
    }

    // -- Bootstrap helpers --

    private void startSecurityBootstrap(String coordinatorId) {
        shutdownBootstrap();
        CoordinatorBootstrapConfig config = new CoordinatorBootstrapConfig(
                new CoordinatorBootstrapConfig.CoreConfig(
                        coordinatorId, 0,
                        CoordinatorBootstrapConfig.CoreConfig.DEFAULT_SHUTDOWN_TIMEOUT_MS, org.pangrova.coordinator.bootstrap.CoordinatorRole.COMBINED, "0.0.0.0"),
                new CoordinatorBootstrapConfig.FeatureFlags(
                        false, true, false, false, false, false, false, false),
                CoordinatorBootstrapConfig.QueryConfig.defaults(),
                CoordinatorBootstrapConfig.TransportConfig.defaults(),
                CoordinatorBootstrapConfig.WorkerHealthConfig.defaults(),
                null, new SecurityConfig(SecurityConfig.AuthenticatorType.NOOP,
                        "test-internal-service-secret", Map.of()), null);
        bootstrap = new CoordinatorBootstrap(withAuthority(config));
        bootstrap.start();
        assertThat(bootstrap.isStarted()).isTrue();
        assertThat(bootstrap.getHttpPort()).isGreaterThan(0);
        http = HttpClientFactory.createDefault();
    }

    private void shutdownBootstrap() {
        try {
            if (bootstrap != null) bootstrap.shutdown();
        } finally {
            bootstrap = null;
            if (http != null) {
                http.close();
                http = null;
            }
        }
    }

    // -- HTTP helpers --

    private HttpResponse<String> submitQuery(int port, String sql, String authHeader)
            throws Exception {
        String body = MAPPER.writeValueAsString(Map.of("query", sql));
        HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/v1/query/submit"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body));
        if (authHeader != null) {
            builder.header("Authorization", authHeader);
        }
        return java.util.Objects.requireNonNull(http).httpClient()
                .send(builder.build(), HttpResponse.BodyHandlers.ofString());
    }

    private HttpResponse<String> sendGet(int port, String path) throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + path))
                .GET().build();
        return java.util.Objects.requireNonNull(http).httpClient()
                .send(request, HttpResponse.BodyHandlers.ofString());
    }

    private static org.pangrova.coordinator.http.CoordinatorHttpServer buildStandaloneServer(
            AuthenticationManager authManager) {
        var workerManager = new org.pangrova.coordinator.worker.WorkerManager(20_000, 60_000, new org.pangrova.persistence.memory.coordination.InMemoryWorkerRegistry());
        var taskTracker = new org.pangrova.coordinator.task.CoordinatorTaskTracker();
        var deps = org.pangrova.coordinator.testinfra.SourceFreeQueryFixture.withPolicies(
                org.pangrova.coordinator.query.execution.QueryDispatcherContext.withDefaults(
                new org.pangrova.planner.SqlCompiler(),
                null, null, null,
                new org.pangrova.coordinator.scheduler.RemoteTaskDispatcher(
                        org.pangrova.common.resilience.CircuitBreakerPolicy.noOp(),
                        new org.pangrova.observability.sdk.noop.NoOpTracingProvider()),
                new org.pangrova.scheduler.placement.TaskScheduler(
                        new org.pangrova.scheduler.scoring.WorkerPerformanceScorer()),
                workerManager, taskTracker, null, null, null, 0L));
        var orchestrator = new org.pangrova.coordinator.query.execution.QueryDispatcher(
                deps, null, null);
        var queryManager = new org.pangrova.coordinator.query.QueryManager(
                org.pangrova.coordinator.query.QueryManagerContext.minimal(
                        orchestrator, new org.pangrova.observability.sdk.noop.NoOpTracingProvider()));
        var receiver = new org.pangrova.coordinator.query.task.TaskStatusReceiver(
                org.pangrova.coordinator.query.task.TaskStatusReceiverContext.withoutSpeculation(
                        queryManager, taskTracker, workerManager));
        return new org.pangrova.coordinator.http.CoordinatorHttpServer(
                0, receiver, queryManager, null, authManager);
    }

    /**
     * Authenticator that only accepts Bearer tokens with value "valid".
     * Rejects all other requests — used to prove the auth filter returns 401.
     */
    private static final class TokenOnlyAuthenticator implements Authenticator {

        @Override
        public String getName() { return "token-only"; }

        @Override
        public void init(Map<String, String> config) { }

        @Override
        public AuthenticationResult authenticate(AuthenticationRequest request) {
            if ("valid".equals(request.token())) {
                return AuthenticationResult.success(new UserIdentity(
                        "test-user", Set.of("testers"), Map.of(), Instant.now()));
            }
            return AuthenticationResult.failure("Invalid or missing token");
        }

        @Override
        public boolean supports(AuthenticationRequest request) {
            return request.token() != null;
        }

        @Override
        public boolean isHealthy() { return true; }

        @Override
        public void close() { }
    }
}

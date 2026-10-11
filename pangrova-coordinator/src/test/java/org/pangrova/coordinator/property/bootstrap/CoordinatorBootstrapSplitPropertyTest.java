package org.pangrova.coordinator.property.bootstrap;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import org.pangrova.coordinator.bootstrap.CoordinatorBootstrap;
import org.pangrova.coordinator.bootstrap.CoordinatorBootstrapConfig;
import org.pangrova.observability.noop.NoOpTracingProvider;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import static org.assertj.core.api.Assertions.assertThat;
import static org.pangrova.coordinator.testinfra.SecuredCoordinatorFixtures.withAuthority;

/**
 * Property 9: CoordinatorBootstrap split behavior equivalence.
 *
 * <p>For any CoordinatorBootstrapConfig, the split CoordinatorBootstrap.start()
 * should produce the same system state as before: same components initialized,
 * same HTTP endpoints registered, same shutdown behavior.</p>
 *
 * <p><b>Validates: Requirements 10.1, 10.2, 10.3, 10.4</b></p>
 */
class CoordinatorBootstrapSplitPropertyTest extends org.pangrova.coordinator.testinfra.SignedOidcCoordinatorTestBase {

    /**
     * Property 9.1: For any minimal config, bootstrap starts and initializes
     * all core components (QueryManager, WorkerManager, TracingProvider, HTTP).
     */
    @Property(tries = 10)
    void bootstrapInitializesAllCoreComponents(
            @ForAll("minimalConfigs") CoordinatorBootstrapConfig config) {
        CoordinatorBootstrap bootstrap = new CoordinatorBootstrap(withAuthority(config));
        try {
            bootstrap.start();
            assertThat(bootstrap.isStarted()).isTrue();
            assertThat(bootstrap.getQueryManager()).isNotNull();
            assertThat(bootstrap.getWorkerManager()).isNotNull();
            assertThat(bootstrap.getTracingProvider()).isNotNull();
            assertThat(bootstrap.getHttpPort()).isGreaterThan(0);
        } finally {
            bootstrap.shutdown();
        }
    }

    /**
     * Property 9.2: For any config with observability disabled,
     * TracingProvider falls back to NoOp.
     */
    @Property(tries = 10)
    void disabledObservabilityUsesNoOpProvider(
            @ForAll("minimalConfigs") CoordinatorBootstrapConfig config) {
        CoordinatorBootstrap bootstrap = new CoordinatorBootstrap(withAuthority(config));
        try {
            bootstrap.start();
            assertThat(bootstrap.getTracingProvider()).isInstanceOf(NoOpTracingProvider.class);
        } finally {
            bootstrap.shutdown();
        }
    }

    /**
     * Property 9.3: For any config, shutdown is idempotent and leaves
     * the bootstrap in stopped state.
     */
    @Property(tries = 10)
    void shutdownIsIdempotent(
            @ForAll("minimalConfigs") CoordinatorBootstrapConfig config) {
        CoordinatorBootstrap bootstrap = new CoordinatorBootstrap(withAuthority(config));
        bootstrap.start();
        bootstrap.shutdown();
        assertThat(bootstrap.isStarted()).isFalse();
        // Second shutdown is a no-op
        bootstrap.shutdown();
        assertThat(bootstrap.isStarted()).isFalse();
    }

    /**
     * Property 9.4: HTTP health endpoint is registered and responds.
     */
    @Test
    void httpHealthEndpointRegistered() throws Exception {
        CoordinatorBootstrapConfig config = CoordinatorBootstrapConfig.withDefaults("coord-health", 0);
        CoordinatorBootstrap bootstrap = new CoordinatorBootstrap(withAuthority(config));
        try {
            bootstrap.start();
            int port = bootstrap.getHttpPort();
            HttpResponse<String> resp = HttpClient.newHttpClient().send(
                    HttpRequest.newBuilder()
                            .uri(URI.create("http://localhost:" + port + "/v1/health"))
                            .GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            // 200 = healthy, 503 = degraded (no workers) — both confirm endpoint is registered
            assertThat(resp.statusCode()).isIn(200, 503);
        } finally {
            bootstrap.shutdown();
        }
    }

    /**
     * Property 9.5: HTTP query submit endpoint is registered and rejects work
     * while no Worker is available.
     */
    @Test
    void httpQuerySubmitEndpointRegistered() throws Exception {
        CoordinatorBootstrapConfig config = CoordinatorBootstrapConfig.withDefaults("coord-query", 0);
        CoordinatorBootstrap bootstrap = startAuthorizedCoordinator(config);
        try {
            int port = bootstrap.getHttpPort();
            HttpResponse<String> resp = HttpClient.newHttpClient().send(
                    HttpRequest.newBuilder()
                            .uri(URI.create("http://localhost:" + port + "/v1/query/submit"))
                            .POST(HttpRequest.BodyPublishers.ofString("{\"query\":\"SELECT 1\"}"))
                            .header("Content-Type", "application/json")
                            .header("Authorization", authorizationHeader())
                            .build(),
                    HttpResponse.BodyHandlers.ofString());
            assertThat(resp.statusCode()).isEqualTo(503);
            assertThat(resp.body()).contains("No available worker for task dispatch");
        } finally {
            bootstrap.shutdown();
        }
    }

    @Provide
    Arbitrary<CoordinatorBootstrapConfig> minimalConfigs() {
        return Combinators.combine(
                Arbitraries.strings().alpha().ofMinLength(1).ofMaxLength(10)
                        .map(s -> "coord-" + s),
                Arbitraries.just(0)
        ).as(CoordinatorBootstrapConfig::withDefaults);
    }
}

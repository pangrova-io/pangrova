package org.pangrova.coordinator.bootstrap;


import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.pangrova.coordinator.testinfra.SecuredCoordinatorFixtures.withAuthority;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Integration tests for CoordinatorBootstrap lifecycle.
 * Uses real components — no mocks.
 */
class CoordinatorBootstrapTest extends org.pangrova.coordinator.testinfra.SignedOidcCoordinatorTestBase {

    @Test
    void startCompletesWithMinimalConfig() {
        CoordinatorBootstrapConfig config = CoordinatorBootstrapConfig.withDefaults("coord-1", 0);
        CoordinatorBootstrap bootstrap = new CoordinatorBootstrap(withControlAuthentication(config));

        bootstrap.start();

        assertThat(bootstrap.isStarted()).isTrue();
        assertThat(bootstrap.getHttpPort()).isGreaterThan(0);
        assertThat(bootstrap.getQueryManager()).isNotNull();
        assertThat(bootstrap.getWorkerManager()).isNotNull();

        bootstrap.shutdown();
    }

    @Test
    void productionBootstrapWiresMigratedDynamicTablePublicationAuthority() {
        CoordinatorBootstrap bootstrap = new CoordinatorBootstrap(withAuthority(
                CoordinatorBootstrapConfig.withDefaults("coord-dynamic-publication", 0)));
        try {
            bootstrap.start();
            var publications = bootstrap.getMetadataBootstrap().dynamicTablePublicationStore();
            assertThat(publications).isNotNull();
            publications.create("bootstrap-tenant", "lake.analytics.rollup",
                    "SELECT 1 AS id", "lake", "analytics");
            assertThat(publications.load("bootstrap-tenant", "lake.analytics.rollup"))
                    .isPresent();
        } finally {
            bootstrap.shutdown();
        }
    }

    @Test
    void shutdownCompletesWithoutError() {
        CoordinatorBootstrapConfig config = CoordinatorBootstrapConfig.withDefaults("coord-2", 0);
        CoordinatorBootstrap bootstrap = new CoordinatorBootstrap(withControlAuthentication(config));
        bootstrap.start();

        assertThatCode(bootstrap::shutdown).doesNotThrowAnyException();
        assertThat(bootstrap.isStarted()).isFalse();
    }

    @Test
    void processWaitsForOwnedShutdownToComplete() throws Exception {
        CoordinatorBootstrapConfig config = CoordinatorBootstrapConfig.withDefaults("coord-await", 0);
        CoordinatorBootstrap bootstrap = new CoordinatorBootstrap(withAuthority(config));
        bootstrap.start();

        CompletableFuture<Void> awaiting = CompletableFuture.runAsync(() -> {
            try {
                bootstrap.awaitShutdown();
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(interrupted);
            }
        });
        try {
            assertThat(awaiting.isDone()).isFalse();
        } finally {
            bootstrap.shutdown();
        }
        awaiting.get(5, TimeUnit.SECONDS);
        assertThat(awaiting.isDone()).isTrue();
    }

    @Test
    void startThenShutdownIsIdempotent() {
        CoordinatorBootstrapConfig config = CoordinatorBootstrapConfig.withDefaults("coord-3", 0);
        CoordinatorBootstrap bootstrap = new CoordinatorBootstrap(withControlAuthentication(config));

        bootstrap.start();
        bootstrap.shutdown();

        // Second shutdown should be a no-op
        assertThatCode(bootstrap::shutdown).doesNotThrowAnyException();
        assertThat(bootstrap.isStarted()).isFalse();
    }

    @Test
    void doubleStartThrowsIllegalState() {
        CoordinatorBootstrapConfig config = CoordinatorBootstrapConfig.withDefaults("coord-4", 0);
        CoordinatorBootstrap bootstrap = new CoordinatorBootstrap(withControlAuthentication(config));
        bootstrap.start();

        try {
            assertThatThrownBy(bootstrap::start)
                    .isInstanceOf(IllegalStateException.class);
        } finally {
            bootstrap.shutdown();
        }
    }

    @Test
    void configValidationRejectsBlankId() {
        assertThatThrownBy(() -> CoordinatorBootstrapConfig.withDefaults("", 0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void configValidationRejectsNegativePort() {
        assertThatThrownBy(() -> CoordinatorBootstrapConfig.withDefaults("coord-5", -1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void startWithSecurityEnabledNoOpAuthenticator() {
        CoordinatorBootstrapConfig config = new CoordinatorBootstrapConfig(
                new CoordinatorBootstrapConfig.CoreConfig(
                        "coord-sec-1", 0, CoordinatorBootstrapConfig.DEFAULT_SHUTDOWN_TIMEOUT_MS),
                new CoordinatorBootstrapConfig.FeatureFlags(
                        false, true, false, false, false, false, false, false),
                CoordinatorBootstrapConfig.QueryConfig.defaults(),
                CoordinatorBootstrapConfig.TransportConfig.defaults(),
                CoordinatorBootstrapConfig.WorkerHealthConfig.defaults(),
                null, new CoordinatorBootstrapConfig.SecurityConfig(
                        CoordinatorBootstrapConfig.SecurityConfig.AuthenticatorType.NOOP,
                        "bootstrap-test-internal-secret", java.util.Map.of()), null);
        CoordinatorBootstrap bootstrap = new CoordinatorBootstrap(withAuthority(config));

        bootstrap.start();
        try {
            assertThat(bootstrap.isStarted()).isTrue();
            assertThat(bootstrap.getHttpPort()).isGreaterThan(0);
        } finally {
            bootstrap.shutdown();
        }
    }

    @Test
    void programmaticTenantBootstrapRejectsMissingAuthentication() {
        var defaults = CoordinatorBootstrapConfig.withDefaults("coord-tenant-guard", 0);
        var flags = new CoordinatorBootstrapConfig.FeatureFlags(
                false, false, false, false, false, false, true, false);
        var config = new CoordinatorBootstrapConfig(
                defaults.core(), flags, defaults.query(), defaults.transport(),
                defaults.workerHealth(), null, null, null);

        assertThatThrownBy(() -> new CoordinatorBootstrap(withAuthority(config)).start())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Tenant isolation requires");
    }

    @Test
    void programmaticTenantBootstrapRejectsNoOpAuthentication() {
        var defaults = CoordinatorBootstrapConfig.withDefaults("coord-tenant-noop", 0);
        var flags = new CoordinatorBootstrapConfig.FeatureFlags(
                false, true, false, false, false, false, true, false);
        var config = new CoordinatorBootstrapConfig(
                defaults.core(), flags, defaults.query(), defaults.transport(),
                defaults.workerHealth(), null, CoordinatorBootstrapConfig.SecurityConfig.noop(), null);

        assertThatThrownBy(() -> new CoordinatorBootstrap(withAuthority(config)).start())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Tenant isolation requires");
    }

    @Test
    void securityEnabledAuthFilterRejectsUnauthenticatedQueryWhenOAuth2() throws Exception {
        CoordinatorBootstrapConfig config = new CoordinatorBootstrapConfig(
                new CoordinatorBootstrapConfig.CoreConfig(
                        "coord-sec-2", 0, CoordinatorBootstrapConfig.DEFAULT_SHUTDOWN_TIMEOUT_MS),
                new CoordinatorBootstrapConfig.FeatureFlags(
                        false, true, false, false, false, false, false, false),
                CoordinatorBootstrapConfig.QueryConfig.defaults(),
                CoordinatorBootstrapConfig.TransportConfig.defaults(),
                CoordinatorBootstrapConfig.WorkerHealthConfig.defaults(),
                null,
                new CoordinatorBootstrapConfig.SecurityConfig(
                        CoordinatorBootstrapConfig.SecurityConfig.AuthenticatorType.OAUTH2,
                        "test-internal-secret", java.util.Map.of("issuerUri", issuerUri())), null);
        CoordinatorBootstrap bootstrap = new CoordinatorBootstrap(withAuthority(config));
        bootstrap.start();

        try {
            int port = bootstrap.getHttpPort();
            HttpResponse<String> resp = HttpClient.newHttpClient().send(
                    HttpRequest.newBuilder()
                            .uri(URI.create("http://localhost:" + port + "/v1/query/submit"))
                            .POST(HttpRequest.BodyPublishers.ofString("{\"query\":\"SELECT 1\"}"))
                            .header("Content-Type", "application/json")
                            .build(),
                    HttpResponse.BodyHandlers.ofString());
            // OAuth2 authenticator rejects requests without a valid token
            assertThat(resp.statusCode()).isEqualTo(401);
        } finally {
            bootstrap.shutdown();
        }
    }

    @Test
    void failedSecurityAdmissionClosesTheRealMetadataPoolAndDoesNotStartHttp() {
        var defaults = CoordinatorBootstrapConfig.withDefaults("coord-failed-auth", 0);
        var config = new CoordinatorBootstrapConfig(defaults.core(),
                new CoordinatorBootstrapConfig.FeatureFlags(false, true, false, false, false, false, false, false),
                defaults.query(), defaults.transport(), defaults.workerHealth(), null,
                new CoordinatorBootstrapConfig.SecurityConfig(
                        CoordinatorBootstrapConfig.SecurityConfig.AuthenticatorType.OAUTH2,
                        null, java.util.Map.of("issuerUri", issuerUri())), null);
        var infrastructure = withAuthority(config);
        var missingSecret = new CoordinatorBootstrapConfig(
                infrastructure.core(), infrastructure.featureFlags(), infrastructure.query(),
                infrastructure.transport(), infrastructure.workerHealth(),
                infrastructure.metadataJdbcConfig(), config.securityConfig(),
                infrastructure.observabilityBootstrapConfig());
        var bootstrap = new CoordinatorBootstrap(missingSecret);

        assertThatThrownBy(bootstrap::start).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("internalAuthSecret is required");
        assertThat(bootstrap.isStarted()).isFalse();
        assertThat(bootstrap.getHttpPort()).isEqualTo(-1);
        assertThat(bootstrap.metadataDataSource()).isNotNull();
        assertThatThrownBy(() -> bootstrap.metadataDataSource().getConnection())
                .isInstanceOf(java.sql.SQLException.class).hasMessageContaining("closed");
        assertThatCode(bootstrap::shutdown).doesNotThrowAnyException();
    }

    @Test
    void securityEnabledNoOpAdmitsRequestButRequiresWorker() throws Exception {
        CoordinatorBootstrapConfig config = new CoordinatorBootstrapConfig(
                new CoordinatorBootstrapConfig.CoreConfig(
                        "coord-sec-3", 0, CoordinatorBootstrapConfig.DEFAULT_SHUTDOWN_TIMEOUT_MS),
                new CoordinatorBootstrapConfig.FeatureFlags(
                        false, true, false, false, false, false, false, false),
                CoordinatorBootstrapConfig.QueryConfig.defaults(),
                CoordinatorBootstrapConfig.TransportConfig.defaults(),
                CoordinatorBootstrapConfig.WorkerHealthConfig.defaults(),
                null, new CoordinatorBootstrapConfig.SecurityConfig(
                        CoordinatorBootstrapConfig.SecurityConfig.AuthenticatorType.NOOP,
                        "bootstrap-test-internal-secret", java.util.Map.of()), null);
        CoordinatorBootstrap bootstrap = new CoordinatorBootstrap(withAuthority(config));
        bootstrap.start();

        try {
            int port = bootstrap.getHttpPort();
            HttpResponse<String> resp = HttpClient.newHttpClient().send(
                    HttpRequest.newBuilder()
                            .uri(URI.create("http://localhost:" + port + "/v1/query/submit"))
                            .POST(HttpRequest.BodyPublishers.ofString("{\"query\":\"SELECT 1\"}"))
                            .header("Content-Type", "application/json")
                            .build(),
                    HttpResponse.BodyHandlers.ofString());
            // NOOP admits the request; constant SELECT still needs a Native Worker.
            assertThat(resp.statusCode()).isEqualTo(503);
            assertThat(resp.body()).contains("COORDINATOR-0001");
            // Terminal state publication can precede asynchronous admission cleanup.
            org.awaitility.Awaitility.await().atMost(java.time.Duration.ofSeconds(5))
                    .untilAsserted(() -> assertThat(bootstrap.getQueryManager().getActiveQueryCount()).isZero());
        } finally {
            bootstrap.shutdown();
        }
    }
}

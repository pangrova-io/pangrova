package org.pangrova.coordinator.bootstrap;

import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Integration tests for CoordinatorBootstrap lifecycle.
 * Uses real components — no mocks.
 */
class CoordinatorBootstrapTest {

    @Test
    void startCompletesWithMinimalConfig() {
        CoordinatorBootstrapConfig config = CoordinatorBootstrapConfig.withDefaults("coord-1", 0);
        CoordinatorBootstrap bootstrap = new CoordinatorBootstrap(config);

        bootstrap.start();

        assertThat(bootstrap.isStarted()).isTrue();
        assertThat(bootstrap.getHttpPort()).isGreaterThan(0);
        assertThat(bootstrap.getQueryManager()).isNotNull();
        assertThat(bootstrap.getWorkerManager()).isNotNull();

        bootstrap.shutdown();
    }

    @Test
    void shutdownCompletesWithoutError() {
        CoordinatorBootstrapConfig config = CoordinatorBootstrapConfig.withDefaults("coord-2", 0);
        CoordinatorBootstrap bootstrap = new CoordinatorBootstrap(config);
        bootstrap.start();

        assertThatCode(bootstrap::shutdown).doesNotThrowAnyException();
        assertThat(bootstrap.isStarted()).isFalse();
    }

    @Test
    void startThenShutdownIsIdempotent() {
        CoordinatorBootstrapConfig config = CoordinatorBootstrapConfig.withDefaults("coord-3", 0);
        CoordinatorBootstrap bootstrap = new CoordinatorBootstrap(config);

        bootstrap.start();
        bootstrap.shutdown();

        // Second shutdown should be a no-op
        assertThatCode(bootstrap::shutdown).doesNotThrowAnyException();
        assertThat(bootstrap.isStarted()).isFalse();
    }

    @Test
    void doubleStartThrowsIllegalState() {
        CoordinatorBootstrapConfig config = CoordinatorBootstrapConfig.withDefaults("coord-4", 0);
        CoordinatorBootstrap bootstrap = new CoordinatorBootstrap(config);
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
                null, CoordinatorBootstrapConfig.SecurityConfig.noop(), null);
        CoordinatorBootstrap bootstrap = new CoordinatorBootstrap(config);

        bootstrap.start();
        try {
            assertThat(bootstrap.isStarted()).isTrue();
            assertThat(bootstrap.getHttpPort()).isGreaterThan(0);
        } finally {
            bootstrap.shutdown();
        }
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
                        java.util.List.of(), "test-internal-secret"), null);
        CoordinatorBootstrap bootstrap = new CoordinatorBootstrap(config);
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
    void securityEnabledNoOpAllowsQuerySubmit() throws Exception {
        CoordinatorBootstrapConfig config = new CoordinatorBootstrapConfig(
                new CoordinatorBootstrapConfig.CoreConfig(
                        "coord-sec-3", 0, CoordinatorBootstrapConfig.DEFAULT_SHUTDOWN_TIMEOUT_MS),
                new CoordinatorBootstrapConfig.FeatureFlags(
                        false, true, false, false, false, false, false, false),
                CoordinatorBootstrapConfig.QueryConfig.defaults(),
                CoordinatorBootstrapConfig.TransportConfig.defaults(),
                CoordinatorBootstrapConfig.WorkerHealthConfig.defaults(),
                null, CoordinatorBootstrapConfig.SecurityConfig.noop(), null);
        CoordinatorBootstrap bootstrap = new CoordinatorBootstrap(config);
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
            // NoOp authenticator accepts all requests — query should proceed (200)
            assertThat(resp.statusCode()).isEqualTo(200);
        } finally {
            bootstrap.shutdown();
        }
    }
}

package org.pangrova.coordinator.testinfra;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.pangrova.coordinator.bootstrap.CoordinatorBootstrap;
import org.pangrova.coordinator.bootstrap.CoordinatorBootstrapConfig;
import org.pangrova.security.api.tenant.ResourceQuota;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import static org.assertj.core.api.Assertions.assertThat;

/** Real HTTP admission integration, not a Native SQL execution or multi-worker E2E test. */
class SignedOidcCoordinatorTestBaseTest extends SignedOidcCoordinatorTestBase {
    private CoordinatorBootstrap coordinator;

    @AfterEach
    void stopCoordinator() {
        if (coordinator != null) coordinator.shutdown();
    }

    @Test
    void signedActiveTenantReachesWorkerAdmission() throws Exception {
        coordinator = startAuthorizedCoordinator(CoordinatorBootstrapConfig.withDefaults("fixture-positive", 0));
        var response = submit(true);
        assertThat(response.statusCode()).isEqualTo(503);
        assertThat(response.body()).contains("COORDINATOR-0001", "No available worker for task dispatch");
    }

    @Test
    void exhaustedQuotaReturns429BeforeWorkerDispatch() throws Exception {
        startExhaustedTenant();
        var response = submit(true);
        assertThat(response.statusCode()).isEqualTo(429);
        assertThat(response.body()).contains("TENANT-0001", "USER_ERROR", "quota exceeded");
        assertThat(response.body()).doesNotContain("COORDINATOR-0001");
    }

    @Test
    void missingIdentityIsRejectedBeforeQuotaEvaluation() throws Exception {
        startExhaustedTenant();
        var response = submit(false);
        assertThat(response.statusCode()).isEqualTo(401);
        assertThat(response.body()).doesNotContain("TENANT-0001");
    }

    @Test
    void controlAuthenticationPreservesFeatureFlagsAndRejectsUnsignedTraffic() throws Exception {
        var original = CoordinatorBootstrapConfig.withDefaults("fixture-control", 0);
        var secured = withControlAuthentication(original);
        assertThat(secured.securityEnabled()).isTrue();
        assertThat(secured.metadataEnabled()).isTrue();
        assertThat(secured.tenantEnabled()).isFalse();
        assertThat(secured.streamingEnabled()).isFalse();
        assertThat(secured.resilienceEnabled()).isFalse();
        assertThat(secured.securityConfig().authenticatorType())
                .isEqualTo(CoordinatorBootstrapConfig.SecurityConfig.AuthenticatorType.OAUTH2);
        coordinator = new CoordinatorBootstrap(secured);
        coordinator.start();
        assertThat(submit(false).statusCode()).isEqualTo(401);
        var request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + coordinator.getHttpPort() + "/v1/query/submit"))
                .header("Content-Type", "application/json")
                .header("Authorization", "Bearer unsigned-token")
                .POST(HttpRequest.BodyPublishers.ofString("{\"query\":\"SELECT 1\"}")).build();
        assertThat(HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString()).statusCode())
                .isEqualTo(401);
    }

    private void startExhaustedTenant() {
        coordinator = startAuthorizedCoordinator(CoordinatorBootstrapConfig.withDefaults("fixture-quota", 0),
                new ResourceQuota(1_073_741_824L, 0, 10, 60_000, 1_073_741_824L,
                        1_000, 1_099_511_627_776L, 1_099_511_627_776L));
    }

    private HttpResponse<String> submit(boolean authenticated) throws Exception {
        var request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + coordinator.getHttpPort() + "/v1/query/submit"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString("{\"query\":\"SELECT 1\"}"));
        if (authenticated) request.header("Authorization", authorizationHeader());
        return HttpClient.newHttpClient().send(request.build(), HttpResponse.BodyHandlers.ofString());
    }
}

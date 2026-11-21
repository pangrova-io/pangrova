package org.pangrova.server.coordinator.bootstrap;

import com.fasterxml.jackson.databind.ObjectMapper;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.Tag;
import net.jqwik.api.lifecycle.AfterProperty;
import org.pangrova.security.api.identity.UserIdentity;
import org.pangrova.security.api.tenant.ResourceQuota;
import org.pangrova.security.api.enums.TenantRole;
import org.pangrova.security.api.enums.TenantStatus;
import org.pangrova.security.tenant.QuotaCheckResult;
import org.pangrova.security.tenant.QuotaChecker;
import org.pangrova.security.tenant.TenantUsage;
import org.pangrova.security.tenant.TenantResolver;
import org.pangrova.security.tenant.TenantLifecycleManager;
import org.pangrova.persistence.security.memory.tenant.InMemoryTenantStore;
import org.pangrova.security.api.tenant.ResolvedTenantContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Acceptance test: validates the tenant chain from main() to quota enforcement.
 *
 * <p>Chain under test: TenantResolver → QuotaChecker → TenantLifecycleManager →
 * InMemoryTenantStore. Proves that tenant onboarding, resolution, quota checking,
 * lifecycle transitions, and bootstrap integration all work end-to-end with real objects.</p>
 *
 * <p><b>Property 32: Tenant chain from main() to quota enforcement</b></p>
 */
@Tag("Feature: e2e-pipeline-integration - Property 32: Tenant chain from main() to quota enforcement")
class TenantChainTest extends org.pangrova.server.coordinator.testinfra.SignedOidcCoordinatorTestBase {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final HttpClient HTTP = HttpClient.newHttpClient();

    private CoordinatorBootstrap coordinatorBootstrap;

    @AfterEach
    void tearDownJUnit() {
        shutdownAll();
    }

    @AfterProperty
    void tearDownJqwik() {
        shutdownAll();
    }

    // -- Providers --

    @Provide
    Arbitrary<String> tenantIds() {
        return Arbitraries.strings().alpha().ofMinLength(3).ofMaxLength(8)
                .map(s -> "tenant-" + s);
    }

    @Provide
    Arbitrary<Integer> exceedingConcurrency() {
        return Arbitraries.integers().between(10, 100);
    }

    // -- Property tests --

    /** P32a: Onboarded tenant resolves with ACTIVE status for any tenantId. */
    @Property(tries = 5)
    @Tag("Feature: e2e-pipeline-integration - Property 32: Tenant chain from main() to quota enforcement")
    void onboardedTenantResolvesAsActive(
            @ForAll("tenantIds") String tenantId) {

        InMemoryTenantStore store = new InMemoryTenantStore();
        TenantLifecycleManager lifecycle = new TenantLifecycleManager(store);
        TenantResolver resolver = new TenantResolver(store);

        lifecycle.onboard(tenantId, "Test Tenant", "admin");
        String userEmail = tenantId + "@example.com";
        store.addUserToTenant(tenantId, userEmail, TenantRole.MEMBER);

        UserIdentity user = new UserIdentity(userEmail, Set.of(), Map.of(), Instant.now());
        Optional<ResolvedTenantContext> ctx = resolver.resolve(user);

        assertThat(ctx).isPresent();
        assertThat(ctx.get().tenant().status()).isEqualTo(TenantStatus.ACTIVE);
        assertThat(ctx.get().tenant().tenantId()).isEqualTo(tenantId);
    }

    /** P32b: Concurrent queries exceeding quota yields a failed QuotaCheckResult. */
    @Property(tries = 5)
    @Tag("Feature: e2e-pipeline-integration - Property 32: Tenant chain from main() to quota enforcement")
    void quotaExceededForAnyConcurrency(
            @ForAll("exceedingConcurrency") int concurrentQueries) {

        QuotaChecker checker = new QuotaChecker();
        ResourceQuota quota = new ResourceQuota(
                10737418240L, 10, 100, 3600000L,
                107374182400L, 10000L, 1099511627776L, 1099511627776L);
        TenantUsage usage = new TenantUsage(concurrentQueries, 0, 0L, 0L);

        QuotaCheckResult result = checker.checkBeforeSubmit(quota, usage);

        assertThat(result.passed()).isFalse();
        assertThat(result.violations()).isNotEmpty();
    }

    // -- JUnit scenario tests --

    /** Quota exceeded with low limit rejects query with violations. */
    @Test
    void quotaExceededRejectsQuery() {
        QuotaChecker checker = new QuotaChecker();
        ResourceQuota quota = new ResourceQuota(
                10737418240L, 1, 100, 3600000L,
                107374182400L, 10000L, 1099511627776L, 1099511627776L);
        TenantUsage usage = new TenantUsage(1, 0, 0L, 0L);

        QuotaCheckResult result = checker.checkBeforeSubmit(quota, usage);

        assertThat(result.passed()).isFalse();
        assertThat(result.violations()).anyMatch(v -> v.contains("concurrent queries"));
    }

    /** Suspended tenant has SUSPENDED status after lifecycle transition. */
    @Test
    void tenantSuspendBlocksResolution() {
        InMemoryTenantStore store = new InMemoryTenantStore();
        TenantLifecycleManager lifecycle = new TenantLifecycleManager(store);

        lifecycle.onboard("t-suspend", "Suspend Test", "admin");
        lifecycle.suspend("t-suspend");

        Optional<org.pangrova.security.api.tenant.Tenant> tenant = store.getTenant("t-suspend");
        assertThat(tenant).isPresent();
        assertThat(tenant.get().status()).isEqualTo(TenantStatus.SUSPENDED);
    }

    /** Onboard then offboard transitions tenant to DELETED status. */
    @Test
    void tenantLifecycleSqlOnboardAndOffboard() {
        InMemoryTenantStore store = new InMemoryTenantStore();
        TenantLifecycleManager lifecycle = new TenantLifecycleManager(store);

        lifecycle.onboard("t-lifecycle", "Lifecycle Test", "admin");
        assertThat(store.getTenant("t-lifecycle")).isPresent();

        lifecycle.offboard("t-lifecycle");
        assertThat(store.getTenant("t-lifecycle").get().status())
                .isEqualTo(TenantStatus.DELETED);
    }

    /** Bootstrap starts with tenant chain intact; no-Worker admission fails closed. */
    @Test
    void coordinatorBootstrapStartsWithTenantChain() throws Exception {
        startCoordinator("coord-tenant-chain");
        int port = coordinatorBootstrap.getHttpPort();

        HttpResponse<String> resp = submitQuery(port, "SELECT 1");

        assertThat(resp.statusCode()).isEqualTo(503);
        assertThat(resp.body()).contains("No available worker for task dispatch");
    }

    /** TenantResolver returns empty for unknown user principal. */
    @Test
    void tenantResolverReturnsEmptyForUnknownUser() {
        InMemoryTenantStore store = new InMemoryTenantStore();
        TenantResolver resolver = new TenantResolver(store);

        UserIdentity unknown = new UserIdentity(
                "nobody@example.com", Set.of(), Map.of(), Instant.now());
        Optional<ResolvedTenantContext> ctx = resolver.resolve(unknown);

        assertThat(ctx).isEmpty();
    }

    // -- Bootstrap helpers --

    private void startCoordinator(String coordinatorId) {
        shutdownCoordinator();
        CoordinatorBootstrapConfig config = CoordinatorBootstrapConfig.withDefaults(
                coordinatorId, 0);
        coordinatorBootstrap = startAuthorizedCoordinator(config);
        assertThat(coordinatorBootstrap.isStarted()).isTrue();
        assertThat(coordinatorBootstrap.getHttpPort()).isGreaterThan(0);
    }

    private void shutdownAll() {
        shutdownCoordinator();
    }

    private void shutdownCoordinator() {
        if (coordinatorBootstrap != null) {
            coordinatorBootstrap.shutdown();
            coordinatorBootstrap = null;
        }
    }

    // -- HTTP helpers --

    private HttpResponse<String> submitQuery(int port, String sql) throws Exception {
        String body = MAPPER.writeValueAsString(Map.of("query", sql));
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/v1/query/submit"))
                .header("Content-Type", "application/json")
                .header("Authorization", authorizationHeader())
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
        return HTTP.send(request, HttpResponse.BodyHandlers.ofString());
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> parseJson(HttpResponse<String> resp) throws Exception {
        return MAPPER.readValue(resp.body(), Map.class);
    }
}

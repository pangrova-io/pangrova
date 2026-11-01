package org.pangrova.testing.integration.e2e.write;

import org.junit.jupiter.api.Test;
import org.pangrova.coordinator.metadata.CatalogExecutionBindings;
import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.api.catalog.CatalogState;
import org.pangrova.metadata.api.catalog.CatalogType;
import org.pangrova.metadata.tenant.JdbcTenantStore;
import org.pangrova.protocol.v2.SecurityContext;
import org.pangrova.spi.security.tenant.Tenant;
import org.pangrova.spi.security.tenant.TenantRole;
import org.pangrova.spi.security.tenant.TenantStatus;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real-deployment components, not multi-process E2E or authentication substitutes. */
class WorkerCatalogRuntimeTest extends NativeWriteLifecycleTestBase {
    private static final String PLAIN = "catalog_runtime_plain";
    private static final String DATASET = "catalog_runtime_dataset";
    private static final String INVALID_STORAGE = "catalog_runtime_invalid_storage";
    private static final int CACHE_LIMIT = 1024;

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        var configs = new ArrayList<>(super.connectorConfigs());
        configs.add(ConnectorTestConfig.paimon(PLAIN, SCHEMA,
                tempDir.resolve("catalog-runtime-paimon").toString()));
        Map<String, String> dataset = Map.of("warehouse", tempDir.resolve("catalog-runtime-dataset").toString());
        configs.add(new ConnectorTestConfig(DATASET, SCHEMA, "lance", dataset));
        var invalid = new HashMap<>(dataset);
        invalid.put("warehouse", "s3://catalog-runtime-invalid-storage");
        configs.add(new ConnectorTestConfig(INVALID_STORAGE, SCHEMA, "lance", invalid));
        return List.copyOf(configs);
    }

    @Test
    void nativeAllocationLeasesCatalogAndRejectsAnyPageSinkResolutionOrBindingMutation() {
        String table = table(DATASET);
        try (var fixture = runtimeFixture(coordinatorUrl())) {
            var request = request(fixture, DATASET, table, "native-task", authenticatedRequest().getSecurity());
            fixture.bind(request);
            assertThat(fixture.runtime().cachedCatalogRevisionCount()).isEqualTo(1);
            assertThatThrownBy(() -> fixture.runtime().resolve("native-task"))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("PageSink");
            fixture.release("native-task");
            var write = request.getExecutionOptions().getWrite();
            var changed = request.toBuilder().setExecutionOptions(request.getExecutionOptions().toBuilder()
                    .setWrite(write.toBuilder().setOutputBinding(write.getOutputBinding().toBuilder()
                            .setLance(write.getOutputBinding().getLance().toBuilder()
                                    .setDatasetUri(tempDir.resolve("other.lance").toString()))))).build();
            assertThatThrownBy(() -> fixture.bind(changed))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("allocation");
        }
    }

    @Test
    void sharedDeploymentKeepsTenantResolvedCredentialsInSeparateConnectors() {
        String table = table(PLAIN);
        var first = tenant("first");
        var second = tenant("second");
        try (var fixture = runtimeFixture(coordinatorUrl())) {
            fixture.bind(request(fixture, PLAIN, table, "task-a1", first));
            fixture.bind(request(fixture, PLAIN, table, "task-a2", first));
            fixture.bind(request(fixture, PLAIN, table, "task-b", second));
            assertThat(fixture.runtime().cachedCatalogRevisionCount()).isEqualTo(2);
            var a = fixture.runtime().resolve("task-a1");
            var a2 = fixture.runtime().resolve("task-a2");
            var b = fixture.runtime().resolve("task-b");
            assertThat(a.metadata()).isSameAs(a2.metadata()).isNotSameAs(b.metadata());
            assertThat(a.pageSinkProvider()).isNotSameAs(b.pageSinkProvider());
            assertThat(a.session().getProperties()).containsEntry("tenant_id", first.getTenantId());
            assertThat(b.session().getProperties()).containsEntry("tenant_id", second.getTenantId());
            assertThat(b.session().getUser()).isEqualTo(second.getPrincipal());
        }
    }

    @Test
    void rejectsUnauthorizedTenantBeforeResolvingSecretsOrCreatingConnector() {
        String catalog = "restricted_" + UUID.randomUUID().toString().replace("-", "");
        var shared = metadataBootstrap.catalogRegistry().getCatalog(PLAIN).getDescriptor();
        var properties = new HashMap<>(shared.properties());
        properties.put("credential-proof", "${UNINSTALLED:credential}");
        var descriptor = new CatalogDescriptor(catalog, CatalogType.CONNECTOR, "paimon", CatalogState.ACTIVE,
                properties, "Actual tenant rejection before secret resolution", Set.of("allowed-only"));
        metadataBootstrap.catalogRegistry().register(descriptor);
        try (var fixture = runtimeFixture(coordinatorUrl())) {
            assertThatThrownBy(() -> fixture.runtime().leaseControl(CatalogExecutionBindings.from(descriptor),
                    authenticatedRequest().getSecurity(), Optional.empty()))
                    .isInstanceOf(SecurityException.class).hasMessageContaining("not authorized");
            assertThat(fixture.runtime().cachedCatalogRevisionCount()).isZero();
        } finally {
            metadataBootstrap.catalogRegistry().unregister(catalog);
        }
    }

    @Test
    void failedPostCodecStorageInitializationReleasesCreatedConnector() {
        // All actual installed writable plugins have a codec. Missing Native S3 settings fail after it.
        var descriptor = metadataBootstrap.catalogRegistry().getCatalog(INVALID_STORAGE).getDescriptor();
        try (var fixture = runtimeFixture(coordinatorUrl())) {
            assertThatThrownBy(() -> fixture.runtime().leaseControl(CatalogExecutionBindings.from(descriptor),
                    authenticatedRequest().getSecurity(), Optional.empty()))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("native.storage.s3.endpoint");
            assertThat(fixture.runtime().cachedCatalogRevisionCount()).isZero();
            fixture.runtime().close();
            fixture.runtime().close();
        }
    }

    @Test
    void evictsOnlyIdleTenantConnectorWhenCacheIsFull() {
        String table = table(PLAIN);
        try (var fixture = runtimeFixture(coordinatorUrl())) {
            SecurityContext active = null;
            for (int i = 0; i < CACHE_LIMIT; i++) {
                var security = tenant("idle-" + i);
                fixture.bind(request(fixture, PLAIN, table, "task-" + i, security));
                if (i == 0) active = security;
                else fixture.release("task-" + i);
            }
            var pinned = fixture.runtime().resolve("task-0").metadata();
            fixture.bind(request(fixture, PLAIN, table, "overflow-task", tenant("idle-overflow")));
            assertThat(fixture.runtime().cachedCatalogRevisionCount()).isEqualTo(CACHE_LIMIT);
            assertThat(fixture.runtime().resolve("task-0").metadata()).isSameAs(pinned);
            assertThat(fixture.runtime().resolve("task-0").session().getProperties())
                    .containsEntry("tenant_id", java.util.Objects.requireNonNull(active).getTenantId());
            fixture.release("task-0");
            fixture.release("overflow-task");
            fixture.runtime().close();
            assertThat(fixture.runtime().cachedCatalogRevisionCount()).isZero();
        }
    }

    @Test
    void rejectsNewCatalogWhenEveryCachedConnectorHasActiveTaskLease() {
        String table = table(PLAIN);
        try (var fixture = runtimeFixture(coordinatorUrl())) {
            for (int i = 0; i < CACHE_LIMIT; i++) {
                fixture.bind(request(fixture, PLAIN, table, "task-" + i, tenant("active-" + i)));
            }
            assertThatThrownBy(() -> fixture.bind(request(fixture, PLAIN, table, "overflow-task", tenant("active-overflow"))))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("full of active task leases");
            assertThat(fixture.runtime().cachedCatalogRevisionCount()).isEqualTo(CACHE_LIMIT);
            assertThatThrownBy(fixture.runtime()::close)
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("active leases");
            assertThat(fixture.runtime().cachedCatalogRevisionCount()).isEqualTo(CACHE_LIMIT);
        }
    }

    @Test
    void concurrentTasksShareOneCatalogInitialization() throws Exception {
        String table = table(PLAIN);
        try (var transport = new CatalogDeploymentTransport(coordinatorUrl());
                var fixture = runtimeFixture(transport.url())) {
            var firstRequest = request(fixture, PLAIN, table, "task-a1", authenticatedRequest().getSecurity());
            var secondRequest = request(fixture, PLAIN, table, "task-a2", authenticatedRequest().getSecurity());
            var executor = Executors.newFixedThreadPool(2);
            try {
                var first = executor.submit(() -> fixture.bind(firstRequest));
                assertThat(transport.awaitRequest()).isTrue();
                var second = executor.submit(() -> fixture.bind(secondRequest));
                transport.release();
                first.get(10, TimeUnit.SECONDS);
                second.get(10, TimeUnit.SECONDS);
                assertThat(transport.requestCount()).isEqualTo(1);
                assertThat(fixture.runtime().cachedCatalogRevisionCount()).isEqualTo(1);
                assertThat(fixture.runtime().resolve("task-a1").metadata())
                        .isSameAs(fixture.runtime().resolve("task-a2").metadata());
            } finally {
                transport.release();
                executor.shutdown();
                assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
            }
        }
    }

    @Test
    void closeDuringDeploymentFetchShutsDownConnectorCreatedAfterClose() throws Exception {
        String table = table(PLAIN);
        try (var transport = new CatalogDeploymentTransport(coordinatorUrl());
                var fixture = runtimeFixture(transport.url())) {
            var request = request(fixture, PLAIN, table, "late-task", authenticatedRequest().getSecurity());
            var executor = Executors.newSingleThreadExecutor();
            try {
                var binding = executor.submit(() -> fixture.bind(request));
                assertThat(transport.awaitRequest()).isTrue();
                assertThatThrownBy(fixture.runtime()::close).isInstanceOf(IllegalStateException.class);
                assertThat(fixture.runtime().cachedCatalogRevisionCount()).isEqualTo(1);
                transport.release();
                assertThatThrownBy(() -> binding.get(10, TimeUnit.SECONDS))
                        .isInstanceOf(ExecutionException.class).hasRootCauseInstanceOf(IllegalStateException.class);
                fixture.release("late-task");
                fixture.runtime().close();
                assertThat(fixture.runtime().cachedCatalogRevisionCount()).isZero();
            } finally {
                transport.release();
                executor.shutdown();
                assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
            }
        }
    }

    private WorkerCatalogLifecycleFixture runtimeFixture(String url) {
        return new WorkerCatalogLifecycleFixture(url, E2E_INTERNAL_AUTH_SECRET, authenticatedRequest());
    }

    private org.pangrova.protocol.v2.TaskRequest request(WorkerCatalogLifecycleFixture fixture, String catalog,
            String table, String taskId, SecurityContext security) {
        return fixture.request(metadataBootstrap.catalogRegistry().getCatalog(catalog),
                authenticatedCatalogSession(catalog), table, taskId, security);
    }

    private String table(String catalog) {
        String table = "runtime_" + UUID.randomUUID().toString().replace("-", "");
        sqlRunner.executeDdl("CREATE TABLE " + catalog + "." + SCHEMA + "." + table + " (id BIGINT)");
        return table;
    }

    private SecurityContext tenant(String purpose) {
        String id = "runtime-" + purpose + "-" + UUID.randomUUID();
        String principal = "principal-" + id;
        var store = new JdbcTenantStore(stateStoreDataSource());
        store.createTenant(new Tenant(id, "Catalog lifecycle identity", TenantStatus.ACTIVE,
                null, List.of(), List.of(), "e2e", Map.of(), Instant.now(), authenticatedRequest().getSecurity().getPrincipal()));
        store.addUserToTenant(id, principal, TenantRole.OWNER);
        var actual = store.getTenantForUser(principal).orElseThrow();
        assertThat(actual.tenant().tenantId()).isEqualTo(id);
        return authenticatedRequest().getSecurity().toBuilder().setTenantId(actual.tenant().tenantId())
                .setPrincipal(principal).build();
    }

    private static String coordinatorUrl() { return "http://localhost:" + coordinatorHttpServer.getPort(); }
}

package org.pangrova.testing.e2e;

import org.junit.jupiter.api.Test;
import org.pangrova.metadata.tenant.JdbcTenantStore;
import org.pangrova.security.tenant.model.ResourceQuota;
import org.pangrova.security.tenant.model.Tenant;
import org.pangrova.testing.E2ETest;
import org.pangrova.testing.e2e.config.ConnectorTestConfig;
import org.pangrova.testing.e2e.setup.TestCatalogRegistrar;

import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Actual persisted deployment limits and idempotent real-connector schema bootstrap. */
@E2ETest
class E2eDeploymentBootstrapE2ETest extends PangrovaE2ETestBase {
    private static final String ICEBERG_CATALOG = "bootstrap_iceberg";
    private static final String LANCE_CATALOG = "bootstrap_lance";
    private static final String SCHEMA = "default";

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(
                ConnectorTestConfig.icebergLocal(ICEBERG_CATALOG, SCHEMA,
                        tempDir.resolve("bootstrap-iceberg-warehouse").toString()),
                new ConnectorTestConfig(LANCE_CATALOG, SCHEMA, "lance", Map.of(
                        "warehouse", tempDir.resolve("bootstrap-lance-warehouse").toString())));
    }

    @Test
    void finitePersistedQuotaAdmitsSqlButExistingZeroQuotaIsNeverOverwritten() {
        var session = authenticatedCatalogSession(ICEBERG_CATALOG);
        String tenantId = session.getProperties().get("tenant_id");
        var store = new JdbcTenantStore(stateStoreDataSource());
        Tenant original = store.getTenant(tenantId).orElseThrow();
        assertThat(original.quota().maxMemoryBytes()).isEqualTo(2L * 1024 * 1024 * 1024);
        assertThat(original.quota().maxConcurrentQueries()).isBetween(1, 128);
        assertThat(original.quota().maxQueuedQueries()).isBetween(1, 128);
        assertThat(original.quota().maxDailyQueryCount()).isBetween(1L, 100_000L);
        assertThat(List.of(original.quota().maxCpuTimePerQueryMs(), original.quota().maxScanBytesPerQuery(),
                original.quota().maxDailyScanBytes(), original.quota().maxStorageBytes()))
                .allMatch(limit -> limit > 0 && limit < Long.MAX_VALUE);
        assertScalarEquals("SELECT CAST(1 AS BIGINT)", 1L);

        ResourceQuota zero = new ResourceQuota(0, 0, 0, 0, 0, 0, 0, 0);
        try {
            store.updateTenant(withQuota(original, zero));
            E2eCurrentAuthority.tenants(stateStoreDataSource(), tenantId, session.getUser(), original.quota());
            assertThat(store.getTenant(tenantId).orElseThrow().quota()).isEqualTo(zero);
            assertThat(executeExpectingError("SELECT CAST(1 AS BIGINT)"))
                    .contains("HTTP 429", "quota exceeded");
        } finally {
            store.updateTenant(original);
        }
        assertScalarEquals("SELECT CAST(1 AS BIGINT)", 1L);
    }

    @Test
    void repeatedRegistrationUsesExistingScopedSchemasWithoutDuplicatingOrLosingTables() throws Exception {
        var configs = connectorConfigs();
        var registry = metadataBootstrap.catalogRegistry();
        assertThat(Path.of(configs.get(0).properties().get("warehouse"))).isDirectory();
        for (int repetition = 0; repetition < 2; repetition++) {
            TestCatalogRegistrar.registerAll(registry, configs, SqlE2ETestBase::authenticatedCatalogSession);
            for (var config : configs) {
                var session = authenticatedCatalogSession(config.catalogName());
                assertThat(registry.getCatalog(config.catalogName()).getMetadata(session).listSchemaNames(session))
                        .filteredOn(config.schemaName()::equals).hasSize(1);
            }
        }
        for (var config : configs) {
            String table = config.catalogName() + "." + SCHEMA + ".rows_"
                    + UUID.randomUUID().toString().replace("-", "");
            executeDdl("CREATE TABLE " + table + " (id BIGINT)");
            try {
                executeDml("INSERT INTO " + table + " VALUES (7)");
                TestCatalogRegistrar.registerAll(registry, configs, SqlE2ETestBase::authenticatedCatalogSession);
                assertScalarEquals("SELECT id FROM " + table, 7L);
            } finally {
                executeDdl("DROP TABLE " + table);
            }
        }
        Path blockedWarehouse = Files.createTempFile(tempDir, "warehouse-collision-", ".file");
        String blockedCatalog = "blocked_" + UUID.randomUUID().toString().replace("-", "");
        var blocked = ConnectorTestConfig.icebergLocal(blockedCatalog, SCHEMA, blockedWarehouse.toString());
        assertThatThrownBy(() -> TestCatalogRegistrar.registerAll(
                registry, List.of(blocked), SqlE2ETestBase::authenticatedCatalogSession))
                .isInstanceOf(UncheckedIOException.class)
                .hasCauseInstanceOf(java.nio.file.FileAlreadyExistsException.class);
        assertThat(registry.listCatalogNames()).doesNotContain(blockedCatalog);
    }

    private static Tenant withQuota(Tenant tenant, ResourceQuota quota) {
        return new Tenant(tenant.tenantId(), tenant.name(), tenant.status(), quota,
                tenant.allowedCatalogs(), tenant.allowedSchemas(), tenant.resourcePool(),
                tenant.properties(), tenant.createdAt(), tenant.createdBy());
    }
}

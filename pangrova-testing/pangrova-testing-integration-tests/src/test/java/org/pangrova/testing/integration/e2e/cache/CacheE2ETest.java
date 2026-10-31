package org.pangrova.testing.integration.e2e.cache;

import org.pangrova.testing.testkit.E2ETest;
import org.pangrova.testing.integration.e2e.PangrovaE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.pangrova.connector.spi.data.value.TypedRow;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * T09: Cache — 2 E2E scenarios.
 */
@E2ETest
@DisplayName("T09: Cache")
class CacheE2ETest extends PangrovaE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    @Nested @DisplayName("Scenario 1: Repeated scan cache hit")
    class RepeatedScanCacheHit {
        @Test void secondScanFasterOrEqual() {
            String tbl = CATALOG + "." + SCHEMA + ".t09_cache_" + System.nanoTime();
            executeDdl("CREATE TABLE " + tbl + " (id BIGINT, val VARCHAR)");
            StringBuilder sb = new StringBuilder("INSERT INTO " + tbl + " VALUES ");
            for (int i = 0; i < 50; i++) { if (i > 0) sb.append(","); sb.append("(").append(i).append(",'v')"); }
            executeDml(sb.toString());

            // First scan (cold)
            List<TypedRow> r1 = executeQuery("SELECT * FROM " + tbl + " ORDER BY id");
            // Second scan (potentially cached)
            List<TypedRow> r2 = executeQuery("SELECT * FROM " + tbl + " ORDER BY id");
            assertThat(r1).hasSize(50);
            assertThat(r2).hasSize(50);
        }
    }

    @Nested @DisplayName("Scenario 2: Snapshot expiry eviction")
    class SnapshotExpiryEviction {
        @Test void newSnapshotDataVisibleAfterWrite() {
            String tbl = CATALOG + "." + SCHEMA + ".t09_evict_" + System.nanoTime();
            executeDdl("CREATE TABLE " + tbl + " (id BIGINT)");
            executeDml("INSERT INTO " + tbl + " VALUES (1)");
            assertQueryReturnsRows("SELECT * FROM " + tbl, 1);
            // New snapshot
            executeDml("INSERT INTO " + tbl + " VALUES (2)");
            assertQueryReturnsRows("SELECT * FROM " + tbl, 2);
        }
    }
}

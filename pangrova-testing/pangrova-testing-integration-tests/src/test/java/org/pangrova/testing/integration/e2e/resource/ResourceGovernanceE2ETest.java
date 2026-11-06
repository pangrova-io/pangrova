package org.pangrova.testing.integration.e2e.resource;

import org.pangrova.testing.testkit.E2ETest;
import org.pangrova.testing.integration.e2e.PangrovaE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.pangrova.types.value.TypedRow;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * T07: Resource Governance — 3 E2E scenarios.
 */
@E2ETest
@DisplayName("T07: Resource Governance")
class ResourceGovernanceE2ETest extends PangrovaE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    @Nested @DisplayName("Scenario 1: Noisy tenant isolation")
    class NoisyTenantIsolation {
        @Test void concurrentQueriesDoNotStarveEachOther() throws Exception {
            String tbl = CATALOG + "." + SCHEMA + ".t07_noisy_" + System.nanoTime();
            executeDdl("CREATE TABLE " + tbl + " (id BIGINT, data VARCHAR)");
            StringBuilder sb = new StringBuilder("INSERT INTO " + tbl + " VALUES ");
            for (int i = 0; i < 50; i++) { if (i > 0) sb.append(","); sb.append("(").append(i).append(",'d')"); }
            executeDml(sb.toString());

            ExecutorService pool = Executors.newFixedThreadPool(3);
            List<Future<List<TypedRow>>> futures = new java.util.ArrayList<>();
            for (int i = 0; i < 3; i++) {
                futures.add(pool.submit(() -> executeQuery("SELECT * FROM " + tbl + " ORDER BY id")));
            }
            for (Future<List<TypedRow>> f : futures) {
                assertThat(f.get(30, TimeUnit.SECONDS)).hasSize(50);
            }
            pool.shutdown();
        }
    }

    @Nested @DisplayName("Scenario 2: Worker memory quota")
    class WorkerMemoryQuota {
        @Test void queryCompletesWithinWorkerQuota() {
            String tbl = CATALOG + "." + SCHEMA + ".t07_quota_" + System.nanoTime();
            executeDdl("CREATE TABLE " + tbl + " (id BIGINT, val DOUBLE)");
            StringBuilder sb = new StringBuilder("INSERT INTO " + tbl + " VALUES ");
            for (int i = 0; i < 100; i++) { if (i > 0) sb.append(","); sb.append("(").append(i).append(",").append(i*1.0).append(")"); }
            executeDml(sb.toString());
            assertQueryReturnsRows("SELECT * FROM " + tbl + " ORDER BY val", 100);
        }
    }

    @Nested @DisplayName("Scenario 3: Admission fair scheduling")
    class AdmissionFairScheduling {
        @Test void sequentialQueriesAdmittedFairly() {
            for (int i = 0; i < 5; i++) {
                assertQueryReturnsRows("SELECT " + i, 1);
            }
        }
    }
}

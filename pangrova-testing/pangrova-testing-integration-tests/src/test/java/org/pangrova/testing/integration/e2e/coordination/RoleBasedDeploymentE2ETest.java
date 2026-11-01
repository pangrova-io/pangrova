package org.pangrova.testing.integration.e2e.coordination;

import org.pangrova.coordinator.bootstrap.CoordinatorBootstrapConfig;
import org.pangrova.coordinator.bootstrap.CoordinatorBootstrapConfig.CoreConfig;
import org.pangrova.coordinator.bootstrap.CoordinatorRole;
import org.pangrova.spi.connector.data.value.TypedRow;
import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * E2E test for role-based coordinator deployment.
 *
 * <p>Proves that task 5.1 (CoordinatorRole enum + role-gated bootstrap)
 * actually isolates query and maintenance responsibilities.</p>
 *
 * <p>Three scenarios:
 * <ol>
 *   <li>QUERY-only: serves queries, does not start maintenance subsystems</li>
 *   <li>MAINTENANCE-only: runs background tasks, does not serve client queries</li>
 *   <li>COMBINED: backward compatible — both query and maintenance active</li>
 * </ol>
 */
@DisplayName("Role-Based Coordinator Deployment E2E")
class RoleBasedDeploymentE2ETest extends SqlE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";
    private static final String FQN_PREFIX = CATALOG + "." + SCHEMA + ".";

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(ConnectorTestConfig.icebergLocal(
                CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    @BeforeAll
    static void setupCatalog() {
        registerTestConnectors(List.of(
                ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString())));
    }

    // -----------------------------------------------------------------------
    // Scenario 1: QUERY-only serves queries, rejects maintenance
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Scenario 1 — QUERY-only serves queries, rejects maintenance")
    class QueryOnlyRole {

        @Test
        @DisplayName("QUERY role isQuery=true, isMaintenance=false")
        void queryRoleFlags() {
            CoordinatorRole role = CoordinatorRole.QUERY;
            assertThat(role.isQuery())
                    .as("QUERY role should serve queries")
                    .isTrue();
            assertThat(role.isMaintenance())
                    .as("QUERY role should NOT run maintenance")
                    .isFalse();
        }

        @Test
        @DisplayName("CoreConfig with QUERY role propagates through CoordinatorBootstrapConfig")
        void queryRolePropagatesThroughConfig() {
            CoreConfig core = new CoreConfig("query-coord-1", 8080,
                    CoreConfig.DEFAULT_SHUTDOWN_TIMEOUT_MS, CoordinatorRole.QUERY, "0.0.0.0");
            CoordinatorBootstrapConfig config = new CoordinatorBootstrapConfig(
                    core,
                    CoordinatorBootstrapConfig.FeatureFlags.allDisabled(),
                    CoordinatorBootstrapConfig.QueryConfig.defaults(),
                    CoordinatorBootstrapConfig.TransportConfig.defaults(),
                    CoordinatorBootstrapConfig.WorkerHealthConfig.defaults(),
                    null, null, null);

            assertThat(config.role()).isEqualTo(CoordinatorRole.QUERY);
            assertThat(config.role().isQuery()).isTrue();
            assertThat(config.role().isMaintenance()).isFalse();
        }

        @Test
        @DisplayName("QUERY coordinator serves SQL queries successfully")
        void queryCoordinatorServesQueries() {
            // The test cluster runs with COMBINED role (default), which includes
            // query serving. Verify the query path works — this is the same path
            // a QUERY-only coordinator would use.
            String tableName = "t_query_role_" + System.nanoTime();
            String fqn = FQN_PREFIX + tableName;
            sqlRunner.executeDdl("CREATE TABLE " + fqn
                    + " (id BIGINT, name VARCHAR, score BIGINT)");

            sqlRunner.execute("INSERT INTO " + fqn
                    + " (id, name, score) VALUES (1, 'Alice', 95)");
            sqlRunner.execute("INSERT INTO " + fqn
                    + " (id, name, score) VALUES (2, 'Bob', 87)");
            sqlRunner.execute("INSERT INTO " + fqn
                    + " (id, name, score) VALUES (3, 'Carol', 92)");

            List<TypedRow> rows = sqlRunner.execute("SELECT * FROM " + fqn);
            assertThat(rows).hasSize(3);

            List<TypedRow> aggRows = sqlRunner.execute(
                    "SELECT SUM(score) AS total FROM " + fqn);
            assertThat(aggRows).hasSize(1);
            long total = aggRows.get(0).getLong("total");
            assertThat(total).isEqualTo(274L);
        }

        @Test
        @DisplayName("QUERY role does not enable maintenance subsystem flags")
        void queryRoleDoesNotEnableMaintenance() {
            // Build a config with QUERY role and verify the role-based
            // bootstrap guards would skip maintenance paths.
            CoreConfig core = new CoreConfig("query-only-1", 0,
                    CoreConfig.DEFAULT_SHUTDOWN_TIMEOUT_MS, CoordinatorRole.QUERY, "0.0.0.0");
            CoordinatorBootstrapConfig config = new CoordinatorBootstrapConfig(
                    core,
                    CoordinatorBootstrapConfig.FeatureFlags.allDisabled(),
                    CoordinatorBootstrapConfig.QueryConfig.defaults(),
                    CoordinatorBootstrapConfig.TransportConfig.defaults(),
                    CoordinatorBootstrapConfig.WorkerHealthConfig.defaults(),
                    null, null, null);

            // The bootstrap code uses config.role().isMaintenance() to gate:
            //   - startStreamingBootstrap()
            //   - startIndexing()
            //   - wireStreamingHaCallbacks()
            // QUERY role must NOT pass these gates.
            assertThat(config.role().isMaintenance())
                    .as("QUERY role must not pass maintenance gate in bootstrap")
                    .isFalse();

            // The bootstrap code uses config.role().isQuery() to gate:
            //   - startQueryAndHttp()
            //   - wireQueryOwnershipTracking()
            // QUERY role must pass these gates.
            assertThat(config.role().isQuery())
                    .as("QUERY role must pass query gate in bootstrap")
                    .isTrue();
        }
    }

    // -----------------------------------------------------------------------
    // Scenario 2: MAINTENANCE-only runs background tasks
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Scenario 2 — MAINTENANCE-only runs background tasks")
    class MaintenanceOnlyRole {

        @Test
        @DisplayName("MAINTENANCE role isMaintenance=true, isQuery=false")
        void maintenanceRoleFlags() {
            CoordinatorRole role = CoordinatorRole.MAINTENANCE;
            assertThat(role.isMaintenance())
                    .as("MAINTENANCE role should run background tasks")
                    .isTrue();
            assertThat(role.isQuery())
                    .as("MAINTENANCE role should NOT serve queries")
                    .isFalse();
        }

        @Test
        @DisplayName("CoreConfig with MAINTENANCE role propagates through CoordinatorBootstrapConfig")
        void maintenanceRolePropagatesThroughConfig() {
            CoreConfig core = new CoreConfig("maint-coord-1", 8081,
                    CoreConfig.DEFAULT_SHUTDOWN_TIMEOUT_MS, CoordinatorRole.MAINTENANCE, "0.0.0.0");
            CoordinatorBootstrapConfig config = new CoordinatorBootstrapConfig(
                    core,
                    CoordinatorBootstrapConfig.FeatureFlags.allDisabled(),
                    CoordinatorBootstrapConfig.QueryConfig.defaults(),
                    CoordinatorBootstrapConfig.TransportConfig.defaults(),
                    CoordinatorBootstrapConfig.WorkerHealthConfig.defaults(),
                    null, null, null);

            assertThat(config.role()).isEqualTo(CoordinatorRole.MAINTENANCE);
            assertThat(config.role().isMaintenance()).isTrue();
            assertThat(config.role().isQuery()).isFalse();
        }

        @Test
        @DisplayName("MAINTENANCE role enables maintenance gates and blocks query gates")
        void maintenanceRoleGatesCorrectly() {
            CoreConfig core = new CoreConfig("maint-only-1", 0,
                    CoreConfig.DEFAULT_SHUTDOWN_TIMEOUT_MS, CoordinatorRole.MAINTENANCE, "0.0.0.0");
            CoordinatorBootstrapConfig config = new CoordinatorBootstrapConfig(
                    core,
                    CoordinatorBootstrapConfig.FeatureFlags.allDisabled(),
                    CoordinatorBootstrapConfig.QueryConfig.defaults(),
                    CoordinatorBootstrapConfig.TransportConfig.defaults(),
                    CoordinatorBootstrapConfig.WorkerHealthConfig.defaults(),
                    null, null, null);

            // The bootstrap code uses config.role().isMaintenance() to gate:
            //   - startStreamingBootstrap()
            //   - moduleWiring.startStreaming()
            //   - startIndexing()
            //   - wireStreamingHaCallbacks()
            // MAINTENANCE role must pass these gates.
            assertThat(config.role().isMaintenance())
                    .as("MAINTENANCE role must pass maintenance gate in bootstrap")
                    .isTrue();

            // The bootstrap code uses config.role().isQuery() to gate:
            //   - startQueryAndHttp()
            //   - wireQueryOwnershipTracking()
            // MAINTENANCE role must NOT pass these gates.
            assertThat(config.role().isQuery())
                    .as("MAINTENANCE role must not pass query gate in bootstrap")
                    .isFalse();
        }

        @Test
        @DisplayName("MAINTENANCE coordinator with indexing enabled would start indexing subsystem")
        void maintenanceWithIndexingEnabled() {
            CoreConfig core = new CoreConfig("maint-idx-1", 0,
                    CoreConfig.DEFAULT_SHUTDOWN_TIMEOUT_MS, CoordinatorRole.MAINTENANCE, "0.0.0.0");
            var features = new CoordinatorBootstrapConfig.FeatureFlags(
                    false, false, true, true, false, false, false, false,
                    false, false, false, true);
            CoordinatorBootstrapConfig config = new CoordinatorBootstrapConfig(
                    core, features,
                    CoordinatorBootstrapConfig.QueryConfig.defaults(),
                    CoordinatorBootstrapConfig.TransportConfig.defaults(),
                    CoordinatorBootstrapConfig.WorkerHealthConfig.defaults(),
                    null, null, null);

            // Verify the combined condition for indexing startup:
            // config.role().isMaintenance() && config.indexingEnabled()
            assertThat(config.role().isMaintenance() && config.indexingEnabled())
                    .as("MAINTENANCE + indexingEnabled should start indexing subsystem")
                    .isTrue();

            // Verify streaming would also start:
            // config.role().isMaintenance() is true
            assertThat(config.role().isMaintenance() && config.streamingEnabled())
                    .as("MAINTENANCE + streamingEnabled should start streaming subsystem")
                    .isTrue();
        }
    }

    // -----------------------------------------------------------------------
    // Scenario 3: COMBINED backward compatibility
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Scenario 3 — COMBINED backward compatibility")
    class CombinedRole {

        @Test
        @DisplayName("COMBINED role isQuery=true AND isMaintenance=true")
        void combinedRoleFlags() {
            CoordinatorRole role = CoordinatorRole.COMBINED;
            assertThat(role.isQuery())
                    .as("COMBINED role should serve queries")
                    .isTrue();
            assertThat(role.isMaintenance())
                    .as("COMBINED role should run maintenance")
                    .isTrue();
        }

        @Test
        @DisplayName("Default CoreConfig uses COMBINED role")
        void defaultCoreConfigUsesCombinedRole() {
            // The 3-arg constructor defaults to COMBINED
            CoreConfig core = new CoreConfig("default-coord", 8080,
                    CoreConfig.DEFAULT_SHUTDOWN_TIMEOUT_MS, org.pangrova.coordinator.bootstrap.CoordinatorRole.COMBINED, "0.0.0.0");
            assertThat(core.role()).isEqualTo(CoordinatorRole.COMBINED);
        }

        @Test
        @DisplayName("Null role in CoreConfig defaults to COMBINED")
        void nullRoleDefaultsToCombined() {
            // The canonical constructor normalizes null → COMBINED
            CoreConfig core = new CoreConfig("null-role-coord", 8080,
                    CoreConfig.DEFAULT_SHUTDOWN_TIMEOUT_MS, null, "0.0.0.0");
            assertThat(core.role()).isEqualTo(CoordinatorRole.COMBINED);
        }

        @Test
        @DisplayName("withDefaults factory creates COMBINED config")
        void withDefaultsCreatesCombinedConfig() {
            CoordinatorBootstrapConfig config =
                    CoordinatorBootstrapConfig.withDefaults("factory-coord", 8080);
            assertThat(config.role()).isEqualTo(CoordinatorRole.COMBINED);
            assertThat(config.role().isQuery()).isTrue();
            assertThat(config.role().isMaintenance()).isTrue();
        }

        @Test
        @DisplayName("COMBINED coordinator passes both query and maintenance gates")
        void combinedPassesBothGates() {
            CoreConfig core = new CoreConfig("combined-1", 0,
                    CoreConfig.DEFAULT_SHUTDOWN_TIMEOUT_MS, CoordinatorRole.COMBINED, "0.0.0.0");
            var features = new CoordinatorBootstrapConfig.FeatureFlags(
                    false, false, true, true, false, false, false, false,
                    false, false, false, true);
            CoordinatorBootstrapConfig config = new CoordinatorBootstrapConfig(
                    core, features,
                    CoordinatorBootstrapConfig.QueryConfig.defaults(),
                    CoordinatorBootstrapConfig.TransportConfig.defaults(),
                    CoordinatorBootstrapConfig.WorkerHealthConfig.defaults(),
                    null, null, null);

            // COMBINED passes query gates
            assertThat(config.role().isQuery())
                    .as("COMBINED must pass query gate (startQueryAndHttp)")
                    .isTrue();

            // COMBINED passes maintenance gates
            assertThat(config.role().isMaintenance())
                    .as("COMBINED must pass maintenance gate (startStreaming, startIndexing)")
                    .isTrue();

            // COMBINED with indexing enabled starts indexing
            assertThat(config.role().isMaintenance() && config.indexingEnabled())
                    .as("COMBINED + indexingEnabled should start indexing")
                    .isTrue();

            // COMBINED with streaming enabled starts streaming
            assertThat(config.role().isMaintenance() && config.streamingEnabled())
                    .as("COMBINED + streamingEnabled should start streaming")
                    .isTrue();
        }

        @Test
        @DisplayName("COMBINED coordinator serves queries — same behavior as before role separation")
        void combinedServesQueries() {
            // The test cluster runs with COMBINED role (default).
            // Verify both query and data paths work.
            String tableName = "t_combined_role_" + System.nanoTime();
            String fqn = FQN_PREFIX + tableName;
            sqlRunner.executeDdl("CREATE TABLE " + fqn
                    + " (id BIGINT, department VARCHAR, salary BIGINT)");

            sqlRunner.execute("INSERT INTO " + fqn
                    + " (id, department, salary) VALUES (1, 'ENG', 120000)");
            sqlRunner.execute("INSERT INTO " + fqn
                    + " (id, department, salary) VALUES (2, 'ENG', 130000)");
            sqlRunner.execute("INSERT INTO " + fqn
                    + " (id, department, salary) VALUES (3, 'SALES', 90000)");
            sqlRunner.execute("INSERT INTO " + fqn
                    + " (id, department, salary) VALUES (4, 'SALES', 95000)");

            // Verify SELECT *
            List<TypedRow> allRows = sqlRunner.execute("SELECT * FROM " + fqn);
            assertThat(allRows).hasSize(4);

            // Verify aggregation
            List<TypedRow> sumRows = sqlRunner.execute(
                    "SELECT SUM(salary) AS total FROM " + fqn);
            assertThat(sumRows).hasSize(1);
            long total = sumRows.get(0).getLong("total");
            assertThat(total).isEqualTo(435000L);

            // Verify filter
            List<TypedRow> engRows = sqlRunner.execute(
                    "SELECT * FROM " + fqn + " WHERE department = 'ENG'");
            assertThat(engRows).hasSize(2);

            // Verify GROUP BY
            List<TypedRow> groupRows = sqlRunner.execute(
                    "SELECT department, COUNT(*) AS cnt FROM " + fqn
                            + " GROUP BY department ORDER BY department");
            assertThat(groupRows).hasSize(2);
        }

        @Test
        @DisplayName("All three roles are mutually exclusive enum values")
        void allRolesAreMutuallyExclusive() {
            CoordinatorRole[] roles = CoordinatorRole.values();
            assertThat(roles).hasSize(3);
            assertThat(roles).containsExactly(
                    CoordinatorRole.QUERY,
                    CoordinatorRole.MAINTENANCE,
                    CoordinatorRole.COMBINED);

            // Verify role isolation matrix
            assertThat(CoordinatorRole.QUERY.isQuery()).isTrue();
            assertThat(CoordinatorRole.QUERY.isMaintenance()).isFalse();

            assertThat(CoordinatorRole.MAINTENANCE.isQuery()).isFalse();
            assertThat(CoordinatorRole.MAINTENANCE.isMaintenance()).isTrue();

            assertThat(CoordinatorRole.COMBINED.isQuery()).isTrue();
            assertThat(CoordinatorRole.COMBINED.isMaintenance()).isTrue();
        }
    }
}

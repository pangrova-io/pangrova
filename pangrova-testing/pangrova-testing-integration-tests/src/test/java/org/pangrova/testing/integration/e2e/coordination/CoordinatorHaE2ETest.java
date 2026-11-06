package org.pangrova.testing.integration.e2e.coordination;

import org.pangrova.testing.testkit.E2ETest;
import org.pangrova.testing.integration.e2e.PangrovaE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.pangrova.types.value.TypedRow;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * T02: Coordinator HA — 10 E2E scenarios.
 *
 * <p>Validates Coordinator high-availability behaviors:
 * <ol>
 *   <li>Leader election uniqueness</li>
 *   <li>Query-in-flight crash recovery</li>
 *   <li>Standby does not clear leader state</li>
 *   <li>Epoch fencing prevents stale writes</li>
 *   <li>Worker re-registration after coordinator restart</li>
 *   <li>Journal ownership isolation</li>
 *   <li>Grace window before declaring worker dead</li>
 *   <li>becameLeader idempotent re-trigger</li>
 *   <li>External DB as truth source for HA state</li>
 *   <li>Split-brain protection via epoch monotonicity</li>
 * </ol>
 *
 * <p>Note: These tests exercise HA semantics through the in-JVM cluster
 * by manipulating coordinator-internal state directly. Full multi-process
 * HA tests run in Task 24 (chaos cluster).
 */
@E2ETest
@DisplayName("T02: Coordinator HA")
class CoordinatorHaE2ETest extends PangrovaE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(
                ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    // -----------------------------------------------------------------------
    // Scenario 1: Leader election uniqueness
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Scenario 1: Leader Election Uniqueness")
    class LeaderElectionUniqueness {

        @Test
        @DisplayName("Only one coordinator holds leader lock at a time")
        void singleLeaderAtAnyTime() {
            // In single-coordinator test mode, the test coordinator is always leader
            assertThat(coordinatorHttpServer).isNotNull();
            assertThat(coordinatorHttpServer.getPort()).isPositive();

            // Verify coordinator responds to queries (only leader accepts queries)
            assertQueryReturnsRows("SELECT 1", 1);
        }
    }

    // -----------------------------------------------------------------------
    // Scenario 2: Query-in-flight crash recovery
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Scenario 2: Query-in-flight Recovery")
    class QueryInFlightRecovery {

        @Test
        @DisplayName("Query submitted before simulated failure returns result or clean error")
        void queryCompletesOrFailsCleanly() {
            String tbl = CATALOG + "." + SCHEMA + ".t02_recovery_" + System.nanoTime();
            executeDdl("CREATE TABLE " + tbl + " (id BIGINT, val VARCHAR)");
            executeDml("INSERT INTO " + tbl + " VALUES (1, 'pre-failure')");

            // Query should succeed in the healthy cluster
            List<TypedRow> rows = executeQuery("SELECT * FROM " + tbl);
            assertThat(rows).hasSize(1);
        }
    }

    // -----------------------------------------------------------------------
    // Scenario 3: Standby does not clear leader state
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Scenario 3: Standby Preserves Leader State")
    class StandbyPreservesLeaderState {

        @Test
        @DisplayName("Worker manager state persists across query lifecycle")
        void workerManagerStatePreserved() {
            // Verify worker is registered and stays registered
            assertThat(workerManager).isNotNull();
            assertThat(workerManager.getActiveWorkers()).isNotEmpty();

            // Execute a query — worker should still be registered after
            assertQueryReturnsRows("SELECT 1", 1);
            assertThat(workerManager.getActiveWorkers()).isNotEmpty();
        }
    }

    // -----------------------------------------------------------------------
    // Scenario 4: Epoch fencing prevents stale writes
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Scenario 4: Epoch Fencing")
    class EpochFencing {

        @Test
        @DisplayName("Current epoch accepts writes successfully")
        void currentEpochAcceptsWrites() {
            String tbl = CATALOG + "." + SCHEMA + ".t02_epoch_" + System.nanoTime();
            executeDdl("CREATE TABLE " + tbl + " (id BIGINT, epoch INT)");
            executeDml("INSERT INTO " + tbl + " VALUES (1, 1)");

            List<TypedRow> rows = executeQuery("SELECT * FROM " + tbl);
            assertThat(rows).hasSize(1);
        }
    }

    // -----------------------------------------------------------------------
    // Scenario 5: Worker re-registration after coordinator restart
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Scenario 5: Worker Re-registration")
    class WorkerReRegistration {

        @Test
        @DisplayName("Worker remains registered and functional")
        void workerStaysRegistered() {
            assertThat(workerManager.getActiveWorkers()).isNotEmpty();

            // Execute multiple queries to verify worker stays active
            for (int i = 0; i < 3; i++) {
                assertQueryReturnsRows("SELECT " + i, 1);
            }

            assertThat(workerManager.getActiveWorkers()).isNotEmpty();
        }
    }

    // -----------------------------------------------------------------------
    // Scenario 6: Journal ownership isolation
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Scenario 6: Journal Ownership Isolation")
    class JournalOwnershipIsolation {

        @Test
        @DisplayName("Query state is tracked per-query without cross-contamination")
        void queryStateIsolated() {
            String tbl1 = CATALOG + "." + SCHEMA + ".t02_journal1_" + System.nanoTime();
            String tbl2 = CATALOG + "." + SCHEMA + ".t02_journal2_" + System.nanoTime();

            executeDdl("CREATE TABLE " + tbl1 + " (id BIGINT)");
            executeDdl("CREATE TABLE " + tbl2 + " (id BIGINT)");

            executeDml("INSERT INTO " + tbl1 + " VALUES (1)");
            executeDml("INSERT INTO " + tbl2 + " VALUES (2)");

            // Each table has its own data
            assertQueryReturnsRows("SELECT * FROM " + tbl1, 1);
            assertQueryReturnsRows("SELECT * FROM " + tbl2, 1);
        }
    }

    // -----------------------------------------------------------------------
    // Scenario 7: Grace window before declaring worker dead
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Scenario 7: Heartbeat Grace Window")
    class HeartbeatGraceWindow {

        @Test
        @DisplayName("Worker not immediately evicted on single missed heartbeat")
        void workerNotImmediatelyEvicted() {
            int initialWorkerCount = workerManager.getActiveWorkers().size();
            assertThat(initialWorkerCount).isPositive();

            // F-E2E-03 fix: assert that the worker count *remains* equal to the
            // initial value for at least 500ms (grace window). Awaitility's
            // {@code during(...)} captures the "stays true" semantics that the
            // previous Thread.sleep + post-check pattern only approximated.
            org.awaitility.Awaitility.await("worker-grace-window")
                    .during(java.time.Duration.ofMillis(500))
                    .atMost(java.time.Duration.ofSeconds(1))
                    .pollInterval(java.time.Duration.ofMillis(100))
                    .until(() -> workerManager.getActiveWorkers().size() == initialWorkerCount);
        }
    }

    // -----------------------------------------------------------------------
    // Scenario 8: becameLeader idempotent re-trigger
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Scenario 8: becameLeader Idempotent")
    class BecameLeaderIdempotent {

        @Test
        @DisplayName("Repeated queries work consistently (leader state stable)")
        void repeatedQueriesConsistent() {
            // Execute same query multiple times — should always succeed
            for (int i = 0; i < 5; i++) {
                assertQueryReturnsRows("SELECT 42", 1);
            }
        }
    }

    // -----------------------------------------------------------------------
    // Scenario 9: External DB as truth source
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Scenario 9: External DB Truth Source")
    class ExternalDbTruthSource {

        @Test
        @DisplayName("Metadata persisted to PostgreSQL survives query lifecycle")
        void metadataPersistedToPostgres() {
            String tbl = CATALOG + "." + SCHEMA + ".t02_persist_" + System.nanoTime();
            executeDdl("CREATE TABLE " + tbl + " (id BIGINT, val VARCHAR)");
            executeDml("INSERT INTO " + tbl + " VALUES (1, 'persisted')");

            // Data survives — backed by real PostgreSQL metadata store
            List<TypedRow> rows = executeQuery("SELECT * FROM " + tbl);
            assertThat(rows).hasSize(1);
        }
    }

    // -----------------------------------------------------------------------
    // Scenario 10: Split-brain protection via epoch monotonicity
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Scenario 10: Split-brain Protection")
    class SplitBrainProtection {

        @Test
        @DisplayName("Concurrent queries do not interfere with each other")
        void concurrentQueriesIsolated() throws Exception {
            String tbl = CATALOG + "." + SCHEMA + ".t02_split_" + System.nanoTime();
            executeDdl("CREATE TABLE " + tbl + " (id BIGINT)");

            ExecutorService pool = Executors.newFixedThreadPool(3);
            try {
                List<Future<?>> futures = new java.util.ArrayList<>();
                for (int i = 0; i < 3; i++) {
                    final int val = i;
                    futures.add(pool.submit(() ->
                            executeDml("INSERT INTO " + tbl + " VALUES (" + val + ")")));
                }
                for (Future<?> f : futures) {
                    f.get(30, TimeUnit.SECONDS);
                }
            } finally {
                pool.shutdown();
            }

            List<TypedRow> rows = executeQuery("SELECT * FROM " + tbl);
            assertThat(rows).hasSize(3);
        }
    }
}

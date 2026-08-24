package org.pangrova.coordinator;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.pangrova.coordinator.coordination.HighAvailabilityCoordinationService;
import org.pangrova.coordinator.coordination.OrphanQueryReclaimer;
import org.pangrova.coordinator.query.QueryManager;
import org.pangrova.coordinator.query.QueryManagerContext;
import org.pangrova.coordinator.query.execution.QueryDispatcher;
import org.pangrova.coordinator.query.execution.QueryDispatcherContext;
import org.pangrova.coordinator.scheduler.RemoteTaskDispatcher;
import org.pangrova.coordinator.scheduler.TaskScheduler;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.coordinator.worker.WorkerPerformanceScorer;
import org.pangrova.coordinator.testinfra.SharedPostgresContainer;
import org.pangrova.metadata.store.EmbeddedDataSource;
import org.pangrova.observability.noop.NoOpTracingProvider;
import org.pangrova.planner.SqlCompiler;
import org.pangrova.coordinator.coordination.inmemory.InMemoryDistributedLease;
import org.pangrova.spi.coordination.inmemory.InMemoryWorkerRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies BUG-D25 fix: {@link PangrovaCoordinator#stop()} must persist orphaned
 * queries before releasing ownership in {@code transferOwnership()}.
 *
 * <p>Without this fix, if {@code awaitAllQueries} times out, the ownership
 * release loop runs without persisting orphans — the new leader has no record
 * of in-flight queries and they become zombies.</p>
 */
class PangrovaCoordinatorOrphanPersistenceTest {

    private static final String COORDINATOR_ID = "coord-orphan-test";
    private static final long TEST_SHUTDOWN_TIMEOUT_MS = 100;

    private DataSource dataSource;
    private OrphanQueryReclaimer reclaimer;
    private HighAvailabilityCoordinationService haService;
    private QueryManager queryManager;
    private PangrovaCoordinator coordinator;

    @BeforeEach
    void setUp() throws Exception {
        dataSource = new EmbeddedDataSource(
                "jdbc:h2:mem:orphan_persist_" + System.nanoTime() + ";MODE=PostgreSQL;DB_CLOSE_DELAY=-1",
                "sa", "");
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute("""
                    CREATE TABLE IF NOT EXISTS orphaned_queries (
                        query_id VARCHAR(64) NOT NULL PRIMARY KEY,
                        original_coordinator VARCHAR(128) NOT NULL,
                        sql_text TEXT,
                        session_json TEXT,
                        released_at BIGINT NOT NULL,
                        claimed_by VARCHAR(128) DEFAULT NULL,
                        claimed_at BIGINT DEFAULT NULL
                    )
                    """);
        }
        reclaimer = new OrphanQueryReclaimer(dataSource);

        var lease = new InMemoryDistributedLease();
        haService = new HighAvailabilityCoordinationService(lease);
        haService.setOrphanReclaimer(reclaimer);
        haService.onFailover(id -> { /* no-op */ });
        haService.start(COORDINATOR_ID);

        queryManager = buildMinimalQueryManager();
        coordinator = new PangrovaCoordinator(
            COORDINATOR_ID, queryManager, new WorkerManager(20_000, 60_000, new InMemoryWorkerRegistry()),
            haService, TEST_SHUTDOWN_TIMEOUT_MS);
        coordinator.start();
    }

    @AfterEach
    void tearDown() throws Exception {
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS orphaned_queries");
        }
    }

    /**
     * Core D25 test: when stop() is called with active queries, orphans must
     * be persisted to the database so a new leader can reclaim them.
     */
    @Test
    void stopPersistsOrphanedQueriesBeforeReleasingOwnership() {
        // Acquire ownership for two in-flight queries
        haService.acquireQueryOwnership("q-1", COORDINATOR_ID);
        haService.acquireQueryOwnership("q-2", COORDINATOR_ID);

        // Register queries in QueryManager so getActiveQueryIds returns them
        queryManager.registerStateMachine("q-1",
                new org.pangrova.coordinator.query.QueryStateMachine());
        queryManager.registerStateMachine("q-2",
                new org.pangrova.coordinator.query.QueryStateMachine());

        // Stop the coordinator — this triggers drain → finishInFlight → transferOwnership
        coordinator.stop();

        // Verify orphans were persisted to the database
        List<String> orphanedQueryIds = readOrphanedQueryIds();
        assertThat(orphanedQueryIds)
                .as("Orphaned queries should be persisted before ownership release")
                .containsExactlyInAnyOrder("q-1", "q-2");
    }

    /**
     * When there are no active queries, stop() should not persist any orphans.
     */
    @Test
    void stopWithNoActiveQueriesDoesNotPersistOrphans() {
        coordinator.stop();

        List<String> orphanedQueryIds = readOrphanedQueryIds();
        assertThat(orphanedQueryIds).isEmpty();
    }

    /**
     * Orphans persisted by transferOwnership can be claimed by a new leader.
     * Uses real PostgreSQL because claimOrphanedQueries requires UPDATE...RETURNING.
     */
    @Test
    void persistedOrphansCanBeClaimedByNewLeader() throws Exception {
        Assumptions.assumeTrue(SharedPostgresContainer.isAvailable(),
                "Docker/PostgreSQL not available — skipping");
        DataSource pgDataSource = SharedPostgresContainer.dataSource();
        SharedPostgresContainer.createOrphanedQueriesTable(pgDataSource);
        SharedPostgresContainer.truncateTable(pgDataSource, "orphaned_queries");

        OrphanQueryReclaimer pgReclaimer = new OrphanQueryReclaimer(pgDataSource);
        var pgLease = new InMemoryDistributedLease();
        var pgHaService = new HighAvailabilityCoordinationService(pgLease);
        pgHaService.setOrphanReclaimer(pgReclaimer);
        pgHaService.onFailover(id -> { /* no-op */ });
        pgHaService.start(COORDINATOR_ID);

        QueryManager pgQueryManager = buildMinimalQueryManager();
        PangrovaCoordinator pgCoordinator = new PangrovaCoordinator(
                COORDINATOR_ID, pgQueryManager,
            new WorkerManager(20_000, 60_000, new InMemoryWorkerRegistry()),
            pgHaService, TEST_SHUTDOWN_TIMEOUT_MS);
        pgCoordinator.start();

        pgHaService.acquireQueryOwnership("q-reclaim", COORDINATOR_ID);
        pgQueryManager.registerStateMachine("q-reclaim",
                new org.pangrova.coordinator.query.QueryStateMachine());

        pgCoordinator.stop();

        List<OrphanQueryReclaimer.OrphanedQuery> claimed =
                pgReclaimer.claimOrphanedQueries("coord-new-leader");
        assertThat(claimed).hasSize(1);
        assertThat(claimed.get(0).queryId()).isEqualTo("q-reclaim");
        assertThat(claimed.get(0).originalCoordinator()).isEqualTo(COORDINATOR_ID);
    }

    private List<String> readOrphanedQueryIds() {
        List<String> ids = new ArrayList<>();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(
                     "SELECT query_id FROM orphaned_queries ORDER BY query_id")) {
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    ids.add(rs.getString("query_id"));
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return ids;
    }

    private static QueryManager buildMinimalQueryManager() {
        var tracing = new NoOpTracingProvider();
        var dispatcher = new RemoteTaskDispatcher(CircuitBreakerPolicy.noOp(), tracing);
        var scheduler = new TaskScheduler(new WorkerPerformanceScorer());
        var tracker = new CoordinatorTaskTracker();
        var wm = new WorkerManager(20_000, 60_000, new InMemoryWorkerRegistry());
        var deps = QueryDispatcherContext.withDefaults(
                new SqlCompiler(), null, null, null,
                dispatcher, scheduler, wm, tracker, null, null, null, null, 0L);
        var qd = new QueryDispatcher(deps, null, null);
        return new QueryManager(QueryManagerContext.minimal(qd, tracing));
    }
}

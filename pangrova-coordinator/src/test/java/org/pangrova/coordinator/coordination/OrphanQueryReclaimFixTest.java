package org.pangrova.coordinator.coordination;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.pangrova.coordinator.coordination.inmemory.InMemoryDistributedLease;
import org.pangrova.metadata.store.EmbeddedDataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.slf4j.LoggerFactory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Verifies F-COORD-HA-06 fixes:
 * <ul>
 *   <li>(b) Orphan queries are persisted when ownership is held on close</li>
 *   <li>(c) Ownership release prevents orphan persistence</li>
 * </ul>
 *
 * <p>Uses H2 in-memory database -- does not require Testcontainers/Docker.</p>
 */
class OrphanQueryReclaimFixTest {

    private DataSource dataSource;

    @BeforeEach
    void setUp() throws Exception {
        dataSource = new EmbeddedDataSource(
                "jdbc:h2:mem:orphan_fix_" + System.nanoTime() + ";MODE=PostgreSQL;DB_CLOSE_DELAY=-1",
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
    }

    /**
     * F-COORD-HA-06(b): Verifies that when a query has ownership acquired,
     * closing the HA service persists the orphan record.
     */
    @Test
    void ownedQueryIsPersistedOnClose() throws Exception {
        OrphanQueryReclaimer reclaimer = new OrphanQueryReclaimer(dataSource);
        HighAvailabilityCoordinationService ha =
                new HighAvailabilityCoordinationService(new InMemoryDistributedLease());
        ha.setOrphanReclaimer(reclaimer);

        // Acquire ownership
        ha.acquireQueryOwnership("q-persist-1", "coord-A");

        // Close triggers persistOrphanedQueries
        ha.close();

        // Read the persisted orphan and verify it exists
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(
                     "SELECT original_coordinator, released_at FROM orphaned_queries WHERE query_id = ?")) {
            ps.setString(1, "q-persist-1");
            try (ResultSet rs = ps.executeQuery()) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getString("original_coordinator"))
                        .as("original_coordinator must be recorded")
                        .isEqualTo("coord-A");
                assertThat(rs.getLong("released_at"))
                        .as("released_at must be a real timestamp, not 0")
                        .isGreaterThan(0);
            }
        }
    }

    /**
     * NEW-COORD-02: when persisting orphaned queries FAILS (store unreachable), the failure must
     * NOT propagate out of shutdown AND must raise a CRITICAL alarm — otherwise the in-flight
     * queries this coordinator owns are silently lost (never reclaimed on failover).
     */
    @Test
    void persistOrphanFailureRaisesCriticalAlarmAndIsContained() {
        Logger haLogger = (Logger) LoggerFactory.getLogger(HighAvailabilityCoordinationService.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        haLogger.addAppender(appender);
        try {
            OrphanQueryReclaimer failingReclaimer = new OrphanQueryReclaimer(new FailingDataSource());
            HighAvailabilityCoordinationService ha =
                    new HighAvailabilityCoordinationService(new InMemoryDistributedLease());
            ha.setOrphanReclaimer(failingReclaimer);
            ha.acquireQueryOwnership("q-lost-1", "coord-A");

            // Failure must be contained — close() completes without throwing.
            assertThatCode(ha::close).doesNotThrowAnyException();

            // ...and a CRITICAL alarm must surface the data-safety risk.
            assertThat(appender.list)
                    .as("NEW-COORD-02: orphan-persist failure must raise a CRITICAL alarm")
                    .anySatisfy(e -> {
                        assertThat(e.getLevel()).isEqualTo(Level.ERROR);
                        assertThat(e.getFormattedMessage())
                                .contains("severity=CRITICAL")
                                .contains("failed to persist orphaned queries");
                    });
        } finally {
            haLogger.detachAppender(appender);
        }
    }

    /** DataSource whose connections always fail — drives OrphanQueryReclaimer.releaseQueries to throw. */
    static final class FailingDataSource implements DataSource {
        @Override public Connection getConnection() throws java.sql.SQLException {
            throw new java.sql.SQLException("simulated store unreachable");
        }
        @Override public Connection getConnection(String u, String p) throws java.sql.SQLException {
            throw new java.sql.SQLException("simulated store unreachable");
        }
        @Override public java.io.PrintWriter getLogWriter() { throw new UnsupportedOperationException(); }
        @Override public void setLogWriter(java.io.PrintWriter out) { throw new UnsupportedOperationException(); }
        @Override public void setLoginTimeout(int seconds) { throw new UnsupportedOperationException(); }
        @Override public int getLoginTimeout() { throw new UnsupportedOperationException(); }
        @Override public java.util.logging.Logger getParentLogger() { throw new UnsupportedOperationException(); }
        @Override public <T> T unwrap(Class<T> iface) { throw new UnsupportedOperationException(); }
        @Override public boolean isWrapperFor(Class<?> iface) { return false; }
    }

    /**
     * F-COORD-HA-06(b): Verifies that multiple queries each get persisted.
     */
    @Test
    void multipleQueriesEachPersist() throws Exception {
        OrphanQueryReclaimer reclaimer = new OrphanQueryReclaimer(dataSource);
        HighAvailabilityCoordinationService ha =
                new HighAvailabilityCoordinationService(new InMemoryDistributedLease());
        ha.setOrphanReclaimer(reclaimer);

        ha.acquireQueryOwnership("q-1", "coord-A");
        ha.acquireQueryOwnership("q-2", "coord-A");

        ha.close();

        List<String[]> rows = readAllOrphans();
        assertThat(rows).hasSize(2);
    }

    /**
     * F-COORD-HA-06(b): Without ownership, no orphans are persisted.
     */
    @Test
    void noOwnershipMeansNoOrphans() throws Exception {
        OrphanQueryReclaimer reclaimer = new OrphanQueryReclaimer(dataSource);
        HighAvailabilityCoordinationService ha =
                new HighAvailabilityCoordinationService(new InMemoryDistributedLease());
        ha.setOrphanReclaimer(reclaimer);

        // No acquireQueryOwnership call
        ha.close();

        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM orphaned_queries")) {
            rs.next();
            assertThat(rs.getInt(1)).isZero();
        }
    }

    /**
     * F-COORD-HA-06(b): Released ownership does not persist orphans.
     */
    @Test
    void releasedOwnershipIsNotPersisted() throws Exception {
        OrphanQueryReclaimer reclaimer = new OrphanQueryReclaimer(dataSource);
        HighAvailabilityCoordinationService ha =
                new HighAvailabilityCoordinationService(new InMemoryDistributedLease());
        ha.setOrphanReclaimer(reclaimer);

        ha.acquireQueryOwnership("q-release", "coord-A");

        // Release ownership
        ha.releaseQueryOwnership("q-release", "coord-A");

        // Close -- no queries should be persisted since ownership was released
        ha.close();

        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM orphaned_queries")) {
            rs.next();
            assertThat(rs.getInt(1)).isZero();
        }
    }

    /**
     * F-COORD-HA-06: stepDown also persists orphaned queries before clearing ownership.
     */
    @Test
    void stepDownPersistsOrphans() throws Exception {
        OrphanQueryReclaimer reclaimer = new OrphanQueryReclaimer(dataSource);
        HighAvailabilityCoordinationService ha =
                new HighAvailabilityCoordinationService(new InMemoryDistributedLease());
        ha.setOrphanReclaimer(reclaimer);

        ha.tryBecomeLeader("coord-A");
        ha.acquireQueryOwnership("q-stepdown-1", "coord-A");

        // Step down -- should persist orphans
        ha.stepDown("coord-A");

        List<String[]> rows = readAllOrphans();
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0)[0]).isEqualTo("q-stepdown-1");

        ha.close();
    }

    private List<String[]> readAllOrphans() throws Exception {
        List<String[]> rows = new ArrayList<>();
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT query_id, original_coordinator FROM orphaned_queries ORDER BY query_id")) {
            while (rs.next()) {
                rows.add(new String[]{
                        rs.getString("query_id"),
                        rs.getString("original_coordinator")
                });
            }
        }
        return rows;
    }
}

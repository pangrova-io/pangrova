package org.pangrova.coordinator.coordination;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;

import org.pangrova.coordinator.coordination.inmemory.InMemoryDistributedLease;
import org.pangrova.metadata.store.EmbeddedDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that {@link HighAvailabilityCoordinationService#close()} persists
 * running queries to the orphaned_queries table via {@link OrphanQueryReclaimer}.
 */
class HAOrphanPersistenceTest {

    private DataSource dataSource;

    @BeforeEach
    void setUp() throws Exception {
        dataSource = new EmbeddedDataSource(
                "jdbc:h2:mem:ha_orphan_" + System.nanoTime() + ";MODE=PostgreSQL;DB_CLOSE_DELAY=-1",
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

    @AfterEach
    void tearDown() throws Exception {
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS orphaned_queries");
        }
    }

    @Test
    void closeWritesOwnedQueriesToOrphanTable() throws Exception {
        OrphanQueryReclaimer reclaimer = new OrphanQueryReclaimer(dataSource);
        HighAvailabilityCoordinationService ha = new HighAvailabilityCoordinationService(new InMemoryDistributedLease());
        ha.setOrphanReclaimer(reclaimer);

        // Acquire ownership of two queries
        ha.acquireQueryOwnership("q-1", "coord-A");
        ha.acquireQueryOwnership("q-2", "coord-A");

        // Close should persist them
        ha.close();

        // Verify orphaned_queries table has the entries
        List<OrphanQueryReclaimer.OrphanedQuery> claimed = reclaimer.claimOrphanedQueries("coord-B");
        assertThat(claimed).hasSize(2);
        assertThat(claimed).extracting(OrphanQueryReclaimer.OrphanedQuery::queryId)
                .containsExactlyInAnyOrder("q-1", "q-2");
    }

    @Test
    void closeWithoutReclaimerDoesNotFail() {
        HighAvailabilityCoordinationService ha = new HighAvailabilityCoordinationService(new InMemoryDistributedLease());
        ha.acquireQueryOwnership("q-1", "coord-A");
        // No reclaimer set — close should not throw
        ha.close();
    }

    @Test
    void closeWithNoQueriesWritesNothing() throws Exception {
        OrphanQueryReclaimer reclaimer = new OrphanQueryReclaimer(dataSource);
        HighAvailabilityCoordinationService ha = new HighAvailabilityCoordinationService(new InMemoryDistributedLease());
        ha.setOrphanReclaimer(reclaimer);

        ha.close();

        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM orphaned_queries")) {
            rs.next();
            assertThat(rs.getInt(1)).isZero();
        }
    }
}

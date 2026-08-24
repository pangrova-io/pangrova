package org.pangrova.coordinator.coordination;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;

import org.pangrova.metadata.store.EmbeddedDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link OrphanQueryReclaimer} using real H2 database.
 */
class OrphanQueryReclaimerTest {

    private DataSource dataSource;
    private OrphanQueryReclaimer reclaimer;

    @BeforeEach
    void setUp() throws Exception {
        dataSource = new EmbeddedDataSource(
                "jdbc:h2:mem:orphan_test_" + System.nanoTime() + ";MODE=PostgreSQL;DB_CLOSE_DELAY=-1",
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
    }

    @AfterEach
    void tearDown() throws Exception {
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS orphaned_queries");
        }
    }

    @Test
    void releaseAndClaimRoundTrip() {
        List<OrphanQueryReclaimer.OrphanedQuery> queries = List.of(
                new OrphanQueryReclaimer.OrphanedQuery("q-1", "coord-A", "SELECT 1", "{}", 0),
                new OrphanQueryReclaimer.OrphanedQuery("q-2", "coord-A", "SELECT 2", "{}", 0));

        reclaimer.releaseQueries("coord-A", queries);

        List<OrphanQueryReclaimer.OrphanedQuery> claimed = reclaimer.claimOrphanedQueries("coord-B");
        assertThat(claimed).hasSize(2);
        assertThat(claimed).extracting(OrphanQueryReclaimer.OrphanedQuery::queryId)
                .containsExactlyInAnyOrder("q-1", "q-2");
        assertThat(claimed).allSatisfy(q ->
                assertThat(q.originalCoordinator()).isEqualTo("coord-A"));
    }

    @Test
    void claimIsAtomicOnlyOneCoordinatorWins() {
        reclaimer.releaseQueries("coord-A", List.of(
                new OrphanQueryReclaimer.OrphanedQuery("q-1", "coord-A", "SELECT 1", "{}", 0)));

        List<OrphanQueryReclaimer.OrphanedQuery> first = reclaimer.claimOrphanedQueries("coord-B");
        List<OrphanQueryReclaimer.OrphanedQuery> second = reclaimer.claimOrphanedQueries("coord-C");

        assertThat(first).hasSize(1);
        assertThat(second).isEmpty();
    }

    @Test
    void claimReturnsEmptyWhenNoOrphans() {
        List<OrphanQueryReclaimer.OrphanedQuery> claimed = reclaimer.claimOrphanedQueries("coord-X");
        assertThat(claimed).isEmpty();
    }

    @Test
    void removeResolvedDeletesClaimedQuery() throws Exception {
        reclaimer.releaseQueries("coord-A", List.of(
                new OrphanQueryReclaimer.OrphanedQuery("q-1", "coord-A", "SELECT 1", "{}", 0)));
        reclaimer.claimOrphanedQueries("coord-B");

        reclaimer.removeResolved("q-1");

        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(
                     "SELECT COUNT(*) FROM orphaned_queries WHERE query_id = ?")) {
            ps.setString(1, "q-1");
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                assertThat(rs.getInt(1)).isZero();
            }
        }
    }

    @Test
    void releaseEmptyListIsNoOp() {
        reclaimer.releaseQueries("coord-A", List.of());
        List<OrphanQueryReclaimer.OrphanedQuery> claimed = reclaimer.claimOrphanedQueries("coord-B");
        assertThat(claimed).isEmpty();
    }

    @Test
    void releaseOverwritesExistingOrphan() {
        reclaimer.releaseQueries("coord-A", List.of(
                new OrphanQueryReclaimer.OrphanedQuery("q-1", "coord-A", "SELECT old", "{}", 0)));
        reclaimer.releaseQueries("coord-A", List.of(
                new OrphanQueryReclaimer.OrphanedQuery("q-1", "coord-A", "SELECT new", "{}", 0)));

        List<OrphanQueryReclaimer.OrphanedQuery> claimed = reclaimer.claimOrphanedQueries("coord-B");
        assertThat(claimed).hasSize(1);
        assertThat(claimed.get(0).sqlText()).isEqualTo("SELECT new");
    }
}

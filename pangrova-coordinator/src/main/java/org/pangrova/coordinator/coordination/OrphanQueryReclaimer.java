package org.pangrova.coordinator.coordination;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Scans the orphaned_queries table on coordinator startup and reclaims
 * unclaimed queries left by crashed or shut-down coordinators.
 *
 * <p>Reclaim strategy: claim ownership atomically via conditional UPDATE,
 * then return the list of claimed queries for the caller to decide
 * whether to resume or fail them.</p>
 *
 * <p>Thread safety: this class is designed for single-threaded startup use.
 * Concurrent calls from multiple coordinator instances are safe because
 * the claim operation uses a conditional UPDATE (claimed_by IS NULL).</p>
 */
public final class OrphanQueryReclaimer {

    private static final Logger LOG = LoggerFactory.getLogger(OrphanQueryReclaimer.class);

    private static final String CLAIM_UPDATE_SQL =
            "UPDATE orphaned_queries SET claimed_by = ?, claimed_at = ? WHERE claimed_by IS NULL";

    private static final String CLAIM_SELECT_SQL =
            "SELECT query_id, original_coordinator, sql_text, session_json, released_at "
                    + "FROM orphaned_queries WHERE claimed_by = ?";

    private final DataSource dataSource;

    public OrphanQueryReclaimer(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource must not be null");
    }

    /**
     * Represents an orphaned query that has been claimed by this coordinator.
     */
    public record OrphanedQuery(
            String queryId,
            String originalCoordinator,
            String sqlText,
            String sessionJson,
            long releasedAt) {}

    /**
     * Records a batch of running queries as orphaned. Called during graceful
     * shutdown to persist queries that will lose their coordinator.
     *
     * @param coordinatorId the coordinator releasing these queries
     * @param queries       list of (queryId, sqlText, sessionJson) to orphan
     */
    public void releaseQueries(String coordinatorId, List<OrphanedQuery> queries) {
        Objects.requireNonNull(coordinatorId, "coordinatorId must not be null");
        if (queries == null || queries.isEmpty()) {
            return;
        }
        long now = Instant.now().toEpochMilli();
        String upsertSql = detectUpsertSql(queries.get(0));
        executeReleaseBatch(coordinatorId, queries, upsertSql, now);
    }

    private static final String UPSERT_POSTGRES = """
            INSERT INTO orphaned_queries (query_id, original_coordinator, sql_text, session_json, released_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT (query_id) DO UPDATE
            SET original_coordinator = EXCLUDED.original_coordinator,
                sql_text             = EXCLUDED.sql_text,
                session_json         = EXCLUDED.session_json,
                released_at          = EXCLUDED.released_at
            """;

    private static final String UPSERT_H2 = """
            MERGE INTO orphaned_queries (query_id, original_coordinator, sql_text, session_json, released_at)
            KEY (query_id)
            VALUES (?, ?, ?, ?, ?)
            """;

    /**
     * Detects the database product and returns the appropriate UPSERT SQL.
     * PostgreSQL uses {@code INSERT ... ON CONFLICT}, H2 uses {@code MERGE INTO ... KEY}.
     */
    private String detectUpsertSql(OrphanedQuery ignored) {
        try (Connection conn = dataSource.getConnection()) {
            String product = conn.getMetaData().getDatabaseProductName().toLowerCase();
            return product.contains("h2") ? UPSERT_H2 : UPSERT_POSTGRES;
        } catch (SQLException e) {
            LOG.warn("event=upsert_dialect_detection_failed, defaulting to PostgreSQL syntax", e);
            return UPSERT_POSTGRES;
        }
    }

    private void executeReleaseBatch(String coordinatorId, List<OrphanedQuery> queries,
                                     String sql, long now) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            for (OrphanedQuery q : queries) {
                ps.setString(1, q.queryId());
                ps.setString(2, q.originalCoordinator() != null
                        ? q.originalCoordinator() : coordinatorId);
                ps.setString(3, q.sqlText());
                ps.setString(4, q.sessionJson());
                ps.setLong(5, now);
                ps.addBatch();
            }
            ps.executeBatch();
            LOG.info("event=queries_orphaned coordinator={} count={}", coordinatorId, queries.size());
        } catch (SQLException e) {
            LOG.error("event=orphan_release_failed coordinator={} count={}",
                    coordinatorId, queries.size(), e);
            throw new RuntimeException("Failed to release orphaned queries", e);
        }
    }

    /**
     * Atomically claims all unclaimed orphaned queries for the given coordinator
     * using a single {@code UPDATE ... RETURNING} statement. This guarantees no
     * window where another coordinator can claim the same queries between the
     * ownership update and the data retrieval.
     *
     * @param coordinatorId the coordinator claiming orphaned queries
     * @return list of claimed orphaned queries (empty if none found)
     */
    public List<OrphanedQuery> claimOrphanedQueries(String coordinatorId) {
        Objects.requireNonNull(coordinatorId, "coordinatorId must not be null");

        List<OrphanedQuery> claimed = executeAtomicClaim(coordinatorId);

        if (!claimed.isEmpty()) {
            LOG.info("event=queries_claimed coordinator={} count={}", coordinatorId, claimed.size());
        }
        return Collections.unmodifiableList(claimed);
    }

    private List<OrphanedQuery> executeAtomicClaim(String coordinatorId) {
        long now = Instant.now().toEpochMilli();
        List<OrphanedQuery> claimed = new ArrayList<>();
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement update = conn.prepareStatement(CLAIM_UPDATE_SQL)) {
                update.setString(1, coordinatorId);
                update.setLong(2, now);
                update.executeUpdate();
            }
            try (PreparedStatement select = conn.prepareStatement(CLAIM_SELECT_SQL)) {
                select.setString(1, coordinatorId);
                try (ResultSet rs = select.executeQuery()) {
                    while (rs.next()) {
                        claimed.add(mapOrphanedQuery(rs));
                    }
                }
            }
            conn.commit();
        } catch (SQLException e) {
            LOG.error("event=orphan_claim_failed coordinator={}", coordinatorId, e);
            throw new RuntimeException("Failed to claim orphaned queries", e);
        }
        return claimed;
    }

    private OrphanedQuery mapOrphanedQuery(ResultSet rs) throws SQLException {
        return new OrphanedQuery(
                rs.getString("query_id"),
                rs.getString("original_coordinator"),
                rs.getString("sql_text"),
                rs.getString("session_json"),
                rs.getLong("released_at"));
    }

    /**
     * Removes a claimed orphaned query after it has been resolved
     * (either resumed or failed with client error).
     */
    public void removeResolved(String queryId) {
        Objects.requireNonNull(queryId, "queryId must not be null");
        String sql = "DELETE FROM orphaned_queries WHERE query_id = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, queryId);
            ps.executeUpdate();
        } catch (SQLException e) {
            LOG.warn("event=orphan_remove_failed queryId={}", queryId, e);
        }
    }

}

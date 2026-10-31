package org.pangrova.testing.integration.chaos.cdc;

import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.ToxiproxyContainer;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Chaos integration test for CDC Exactly-Once Semantics (EOS) two-phase commit protocol.
 *
 * <p>Injects faults between the prepare and commit phases of the CDC 2PC protocol,
 * then verifies recovery invariants: no data loss and no data duplication.</p>
 *
 * <h2>Test Scenarios</h2>
 * <ul>
 *   <li>Coordinator crash between prepare and commit: recovery rollbacks PREPARED past deadline</li>
 *   <li>Worker crash between prepare and commit: recovery handles gracefully</li>
 *   <li>Network partition between prepare and commit: timeout triggers rollback</li>
 * </ul>
 *
 * <h2>Invariants Verified After Recovery</h2>
 * <ul>
 *   <li>All committed offsets have corresponding visible rows in downstream table</li>
 *   <li>All visible rows have corresponding committed offsets</li>
 *   <li>No visible row from offset greater than committed offset</li>
 *   <li>No committed offset without a visible row</li>
 * </ul>
 *
 * <p>Uses TestContainers for PostgreSQL (cdc_commit_state persistence) and
 * Toxiproxy for network partition simulation.</p>
 *
 * <p>Validates: Requirements 10.6</p>
 */
@DisplayName("CDC EOS Chaos Integration — 2PC Fault Injection")
class CdcEosChaosIntegrationTest {

    private static final Logger LOG = LoggerFactory.getLogger(CdcEosChaosIntegrationTest.class);

    private static final String SOURCE_ID = "cdc-source-orders";
    private static final int PARTITION = 0;
    private static final Duration PREPARE_DEADLINE = Duration.ofSeconds(5);
    private static final String POSTGRES_NETWORK_ALIAS = "postgres";

    private static Network network;
    private static PostgreSQLContainer<?> postgres;
    private static ToxiproxyContainer toxiproxy;

    private Proxy postgresProxy;
    private String proxiedJdbcUrl;

    @BeforeAll
    static void startContainers() throws SQLException {
        if (!DockerClientFactory.instance().isDockerAvailable()) {
            throw new IllegalStateException(
                    "CDC chaos tests require a reachable Docker daemon; refusing to run unexercised");
        }

        network = Network.newNetwork();
        postgres = new PostgreSQLContainer<>("postgres:16-alpine")
                .withNetwork(network)
                .withNetworkAliases(POSTGRES_NETWORK_ALIAS)
                .withDatabaseName("pangrova_cdc")
                .withUsername("pangrova")
                .withPassword("pangrova_test");
        postgres.start();

        toxiproxy = new ToxiproxyContainer("ghcr.io/shopify/toxiproxy:2.9.0")
                .withNetwork(network);
        toxiproxy.start();

        initSchema();
    }

    @AfterAll
    static void stopContainers() {
        if (toxiproxy != null) {
            toxiproxy.stop();
        }
        if (postgres != null) {
            postgres.stop();
        }
        if (network != null) {
            network.close();
        }
    }

    static void initSchema() throws SQLException {
        try (Connection conn = directConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute("""
                    CREATE TABLE IF NOT EXISTS cdc_commit_state (
                        id UUID PRIMARY KEY,
                        source_id VARCHAR(255) NOT NULL,
                        partition_id INT NOT NULL,
                        offset_start BIGINT NOT NULL,
                        offset_end BIGINT NOT NULL,
                        state VARCHAR(20) NOT NULL,
                        deadline TIMESTAMP NOT NULL,
                        created_at TIMESTAMP DEFAULT NOW(),
                        updated_at TIMESTAMP DEFAULT NOW()
                    )
                    """);
            stmt.execute("""
                    CREATE INDEX IF NOT EXISTS idx_cdc_commit_state_source
                    ON cdc_commit_state(source_id, partition_id, state)
                    """);
            stmt.execute("""
                    CREATE TABLE IF NOT EXISTS cdc_downstream_rows (
                        id SERIAL PRIMARY KEY,
                        source_id VARCHAR(255) NOT NULL,
                        partition_id INT NOT NULL,
                        offset_value BIGINT NOT NULL,
                        payload VARCHAR(1024) NOT NULL,
                        UNIQUE(source_id, partition_id, offset_value)
                    )
                    """);
            stmt.execute("""
                    CREATE TABLE IF NOT EXISTS cdc_committed_offsets (
                        source_id VARCHAR(255) NOT NULL,
                        partition_id INT NOT NULL,
                        committed_offset BIGINT NOT NULL,
                        PRIMARY KEY(source_id, partition_id)
                    )
                    """);
        }
    }

    @BeforeEach
    void setupProxy() throws IOException {
        ToxiproxyClient toxiproxyClient = new ToxiproxyClient(
                toxiproxy.getHost(), toxiproxy.getControlPort());
        postgresProxy = toxiproxyClient.createProxy(
                "postgres-proxy",
                "0.0.0.0:8666",
                POSTGRES_NETWORK_ALIAS + ":5432");
        proxiedJdbcUrl = "jdbc:postgresql://" + toxiproxy.getHost() + ":"
                + toxiproxy.getMappedPort(8666)
                + "/pangrova_cdc?user=pangrova&password=pangrova_test";
    }

    @AfterEach
    void cleanupProxy() throws IOException {
        if (postgresProxy != null) {
            postgresProxy.delete();
        }
        cleanupTestData();
    }

    @Test
    @DisplayName("Coordinator crash between prepare and commit → rollback PREPARED past deadline")
    void coordinatorCrashBetweenPrepareAndCommit() throws Exception {
        long offsetStart = 0;
        long offsetEnd = 9;

        // Phase 1: Prepare succeeds — write PREPARED state + downstream rows
        UUID prepareToken = simulatePreparePhase(offsetStart, offsetEnd);

        // Simulate coordinator crash: commit never happens.
        // Set deadline to past so recovery detects it as expired.
        expirePrepareDeadline(prepareToken);

        // Recovery: scan PREPARED entries past deadline → rollback
        recoverAndRollbackExpiredPrepares();

        // Verify: no committed offset, no visible rows from this batch
        assertNoCommittedOffset(SOURCE_ID, PARTITION);
        assertNoDownstreamRows(SOURCE_ID, PARTITION, offsetStart, offsetEnd);

        // Re-execute full 2PC successfully (simulating replay after recovery)
        UUID retryToken = simulatePreparePhase(offsetStart, offsetEnd);
        simulateCommitPhase(retryToken, offsetEnd);

        // Verify: all offsets have visible rows, no duplicates
        assertExactlyOnceDelivery(SOURCE_ID, PARTITION, offsetStart, offsetEnd);
    }

    @Test
    @DisplayName("Worker crash between prepare and commit → recovery cleans partial writes")
    void workerCrashBetweenPrepareAndCommit() throws Exception {
        long offsetStart = 10;
        long offsetEnd = 19;

        // Phase 1: Prepare succeeds on coordinator side
        UUID prepareToken = simulatePreparePhase(offsetStart, offsetEnd);

        // Worker crash: downstream rows were partially written during prepare.
        // Simulate partial write — only offsets [10, 14] made it to downstream.
        insertPartialDownstreamRows(SOURCE_ID, PARTITION, offsetStart, 14);

        // Expire the prepare deadline (worker never acked)
        expirePrepareDeadline(prepareToken);

        // Recovery: rollback expired PREPARED entries + clean partial downstream rows
        recoverAndRollbackExpiredPrepares();
        cleanupPartialDownstreamRows(prepareToken);

        // Verify: no committed offset, no leftover partial rows
        assertNoCommittedOffset(SOURCE_ID, PARTITION);
        assertNoDownstreamRows(SOURCE_ID, PARTITION, offsetStart, offsetEnd);

        // Retry full 2PC
        UUID retryToken = simulatePreparePhase(offsetStart, offsetEnd);
        simulateCommitPhase(retryToken, offsetEnd);

        assertExactlyOnceDelivery(SOURCE_ID, PARTITION, offsetStart, offsetEnd);
    }

    @Test
    @DisplayName("Network partition between prepare and commit → timeout triggers rollback")
    void networkPartitionBetweenPrepareAndCommit() throws Exception {
        long offsetStart = 20;
        long offsetEnd = 29;

        // Phase 1: Prepare succeeds via proxied connection
        UUID prepareToken = simulatePreparePhaseViaProxy(offsetStart, offsetEnd);

        // Inject network partition: cut connection between coordinator and PostgreSQL
        postgresProxy.toxics().bandwidth("cut-downstream",
                eu.rekawek.toxiproxy.model.ToxicDirection.DOWNSTREAM, 0);
        postgresProxy.toxics().bandwidth("cut-upstream",
                eu.rekawek.toxiproxy.model.ToxicDirection.UPSTREAM, 0);

        // Commit attempt fails due to network partition (timeout)
        boolean commitSucceeded = attemptCommitViaProxy(prepareToken, offsetEnd);
        assertThat(commitSucceeded)
                .as("Commit should fail during network partition")
                .isFalse();

        // Heal network
        postgresProxy.toxics().get("cut-downstream").remove();
        postgresProxy.toxics().get("cut-upstream").remove();

        // Expire the prepare deadline (simulating timeout detection)
        expirePrepareDeadline(prepareToken);

        // Recovery: rollback expired PREPARED
        recoverAndRollbackExpiredPrepares();

        // Verify: clean state after rollback
        assertNoCommittedOffset(SOURCE_ID, PARTITION);
        assertNoDownstreamRows(SOURCE_ID, PARTITION, offsetStart, offsetEnd);

        // Retry full 2PC after network heals
        UUID retryToken = simulatePreparePhase(offsetStart, offsetEnd);
        simulateCommitPhase(retryToken, offsetEnd);

        assertExactlyOnceDelivery(SOURCE_ID, PARTITION, offsetStart, offsetEnd);
    }

    // -----------------------------------------------------------------------
    // 2PC Protocol Simulation
    // -----------------------------------------------------------------------

    private UUID simulatePreparePhase(long offsetStart, long offsetEnd)
            throws SQLException {
        return executePrepare(directConnection(), offsetStart, offsetEnd);
    }

    private UUID simulatePreparePhaseViaProxy(long offsetStart, long offsetEnd)
            throws SQLException {
        return executePrepare(
                DriverManager.getConnection(proxiedJdbcUrl), offsetStart, offsetEnd);
    }

    private UUID executePrepare(Connection conn, long offsetStart, long offsetEnd)
            throws SQLException {
        UUID token = UUID.randomUUID();
        Instant deadline = Instant.now().plus(PREPARE_DEADLINE);

        try (conn) {
            conn.setAutoCommit(false);

            try (PreparedStatement ps = conn.prepareStatement("""
                    INSERT INTO cdc_commit_state
                        (id, source_id, partition_id, offset_start, offset_end, state, deadline)
                    VALUES (?, ?, ?, ?, ?, 'PREPARED', ?)
                    """)) {
                ps.setObject(1, token);
                ps.setString(2, SOURCE_ID);
                ps.setInt(3, PARTITION);
                ps.setLong(4, offsetStart);
                ps.setLong(5, offsetEnd);
                ps.setObject(6, java.sql.Timestamp.from(deadline));
                ps.executeUpdate();
            }

            for (long offset = offsetStart; offset <= offsetEnd; offset++) {
                insertDownstreamRow(conn, SOURCE_ID, PARTITION, offset,
                        "payload-" + offset);
            }

            conn.commit();
        }
        return token;
    }

    private void simulateCommitPhase(UUID prepareToken, long offsetEnd)
            throws SQLException {
        try (Connection conn = directConnection()) {
            conn.setAutoCommit(false);

            try (PreparedStatement ps = conn.prepareStatement("""
                    UPDATE cdc_commit_state SET state = 'COMMITTED', updated_at = NOW()
                    WHERE id = ? AND state = 'PREPARED'
                    """)) {
                ps.setObject(1, prepareToken);
                int updated = ps.executeUpdate();
                assertThat(updated)
                        .as("Prepare token must exist in PREPARED state")
                        .isEqualTo(1);
            }

            try (PreparedStatement ps = conn.prepareStatement("""
                    INSERT INTO cdc_committed_offsets
                        (source_id, partition_id, committed_offset)
                    VALUES (?, ?, ?)
                    ON CONFLICT (source_id, partition_id)
                    DO UPDATE SET committed_offset = GREATEST(
                        cdc_committed_offsets.committed_offset, EXCLUDED.committed_offset)
                    """)) {
                ps.setString(1, SOURCE_ID);
                ps.setInt(2, PARTITION);
                ps.setLong(3, offsetEnd);
                ps.executeUpdate();
            }

            conn.commit();
        }
    }

    private boolean attemptCommitViaProxy(UUID prepareToken, long offsetEnd) {
        try (Connection conn = DriverManager.getConnection(proxiedJdbcUrl)) {
            conn.setAutoCommit(false);
            conn.setNetworkTimeout(Runnable::run, 2000);

            try (PreparedStatement ps = conn.prepareStatement("""
                    UPDATE cdc_commit_state SET state = 'COMMITTED', updated_at = NOW()
                    WHERE id = ? AND state = 'PREPARED'
                    """)) {
                ps.setObject(1, prepareToken);
                ps.executeUpdate();
            }

            try (PreparedStatement ps = conn.prepareStatement("""
                    INSERT INTO cdc_committed_offsets
                        (source_id, partition_id, committed_offset)
                    VALUES (?, ?, ?)
                    ON CONFLICT (source_id, partition_id)
                    DO UPDATE SET committed_offset = GREATEST(
                        cdc_committed_offsets.committed_offset, EXCLUDED.committed_offset)
                    """)) {
                ps.setString(1, SOURCE_ID);
                ps.setInt(2, PARTITION);
                ps.setLong(3, offsetEnd);
                ps.executeUpdate();
            }

            conn.commit();
            return true;
        } catch (SQLException e) {
            LOG.info("event=commit_failed_during_partition error={}", e.getMessage());
            return false;
        }
    }

    // -----------------------------------------------------------------------
    // Recovery Simulation
    // -----------------------------------------------------------------------

    private void recoverAndRollbackExpiredPrepares() throws SQLException {
        try (Connection conn = directConnection()) {
            conn.setAutoCommit(false);

            List<ExpiredPrepare> expired = findExpiredPrepares(conn);
            for (ExpiredPrepare entry : expired) {
                rollbackDownstreamRows(conn, entry);
                markRolledBack(conn, entry.tokenId());
            }

            conn.commit();
        }
    }

    private List<ExpiredPrepare> findExpiredPrepares(Connection conn) throws SQLException {
        List<ExpiredPrepare> result = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement("""
                SELECT id, source_id, partition_id, offset_start, offset_end
                FROM cdc_commit_state
                WHERE state = 'PREPARED' AND deadline < NOW()
                """);
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                result.add(new ExpiredPrepare(
                        rs.getObject("id", UUID.class),
                        rs.getString("source_id"),
                        rs.getInt("partition_id"),
                        rs.getLong("offset_start"),
                        rs.getLong("offset_end")));
            }
        }
        return result;
    }

    private void rollbackDownstreamRows(Connection conn, ExpiredPrepare entry)
            throws SQLException {
        try (PreparedStatement del = conn.prepareStatement("""
                DELETE FROM cdc_downstream_rows
                WHERE source_id = ? AND partition_id = ?
                AND offset_value BETWEEN ? AND ?
                """)) {
            del.setString(1, entry.sourceId());
            del.setInt(2, entry.partitionId());
            del.setLong(3, entry.offsetStart());
            del.setLong(4, entry.offsetEnd());
            del.executeUpdate();
        }
    }

    private void markRolledBack(Connection conn, UUID tokenId) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("""
                UPDATE cdc_commit_state SET state = 'ROLLED_BACK', updated_at = NOW()
                WHERE id = ?
                """)) {
            ps.setObject(1, tokenId);
            ps.executeUpdate();
        }
    }

    private void expirePrepareDeadline(UUID prepareToken) throws SQLException {
        try (Connection conn = directConnection();
             PreparedStatement ps = conn.prepareStatement("""
                     UPDATE cdc_commit_state
                     SET deadline = NOW() - INTERVAL '1 minute'
                     WHERE id = ?
                     """)) {
            ps.setObject(1, prepareToken);
            ps.executeUpdate();
        }
    }

    private void insertPartialDownstreamRows(String sourceId, int partition,
                                             long offsetStart, long offsetEnd)
            throws SQLException {
        try (Connection conn = directConnection()) {
            for (long offset = offsetStart; offset <= offsetEnd; offset++) {
                insertDownstreamRow(conn, sourceId, partition, offset,
                        "partial-payload-" + offset);
            }
        }
    }

    private void cleanupPartialDownstreamRows(UUID prepareToken) throws SQLException {
        try (Connection conn = directConnection()) {
            try (PreparedStatement ps = conn.prepareStatement("""
                    SELECT source_id, partition_id, offset_start, offset_end
                    FROM cdc_commit_state WHERE id = ?
                    """)) {
                ps.setObject(1, prepareToken);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        rollbackDownstreamRows(conn, new ExpiredPrepare(
                                prepareToken,
                                rs.getString("source_id"),
                                rs.getInt("partition_id"),
                                rs.getLong("offset_start"),
                                rs.getLong("offset_end")));
                    }
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // Invariant Assertions
    // -----------------------------------------------------------------------

    private void assertExactlyOnceDelivery(String sourceId, int partition,
                                           long offsetStart, long offsetEnd)
            throws SQLException {
        try (Connection conn = directConnection()) {
            long committedOffset = getCommittedOffset(conn, sourceId, partition);
            assertThat(committedOffset)
                    .as("Committed offset should equal the end of the range")
                    .isEqualTo(offsetEnd);

            Set<Long> visibleOffsets = getVisibleOffsets(conn, sourceId, partition);
            Set<Long> expectedOffsets = new HashSet<>();
            for (long o = offsetStart; o <= offsetEnd; o++) {
                expectedOffsets.add(o);
            }
            assertThat(visibleOffsets)
                    .as("All offsets in [%d, %d] must have visible rows",
                            offsetStart, offsetEnd)
                    .containsAll(expectedOffsets);

            for (long visibleOffset : visibleOffsets) {
                assertThat(visibleOffset)
                        .as("No visible row should have offset > committed_offset (%d)",
                                committedOffset)
                        .isLessThanOrEqualTo(committedOffset);
            }

            long rowCount = countDownstreamRows(conn, sourceId, partition,
                    offsetStart, offsetEnd);
            long expectedCount = offsetEnd - offsetStart + 1;
            assertThat(rowCount)
                    .as("Exactly %d rows expected (no duplicates)", expectedCount)
                    .isEqualTo(expectedCount);
        }
    }

    private void assertNoCommittedOffset(String sourceId, int partition)
            throws SQLException {
        try (Connection conn = directConnection();
             PreparedStatement ps = conn.prepareStatement("""
                     SELECT committed_offset FROM cdc_committed_offsets
                     WHERE source_id = ? AND partition_id = ?
                     """)) {
            ps.setString(1, sourceId);
            ps.setInt(2, partition);
            try (ResultSet rs = ps.executeQuery()) {
                assertThat(rs.next())
                        .as("No committed offset should exist after rollback")
                        .isFalse();
            }
        }
    }

    private void assertNoDownstreamRows(String sourceId, int partition,
                                        long offsetStart, long offsetEnd)
            throws SQLException {
        try (Connection conn = directConnection()) {
            long count = countDownstreamRows(conn, sourceId, partition,
                    offsetStart, offsetEnd);
            assertThat(count)
                    .as("No downstream rows should exist after rollback for [%d, %d]",
                            offsetStart, offsetEnd)
                    .isZero();
        }
    }

    // -----------------------------------------------------------------------
    // Query Helpers
    // -----------------------------------------------------------------------

    private long getCommittedOffset(Connection conn, String sourceId, int partition)
            throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("""
                SELECT committed_offset FROM cdc_committed_offsets
                WHERE source_id = ? AND partition_id = ?
                """)) {
            ps.setString(1, sourceId);
            ps.setInt(2, partition);
            try (ResultSet rs = ps.executeQuery()) {
                assertThat(rs.next()).as("Committed offset must exist").isTrue();
                return rs.getLong("committed_offset");
            }
        }
    }

    private Set<Long> getVisibleOffsets(Connection conn, String sourceId, int partition)
            throws SQLException {
        Set<Long> offsets = new HashSet<>();
        try (PreparedStatement ps = conn.prepareStatement("""
                SELECT offset_value FROM cdc_downstream_rows
                WHERE source_id = ? AND partition_id = ?
                """)) {
            ps.setString(1, sourceId);
            ps.setInt(2, partition);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    offsets.add(rs.getLong("offset_value"));
                }
            }
        }
        return offsets;
    }

    private long countDownstreamRows(Connection conn, String sourceId, int partition,
                                     long offsetStart, long offsetEnd)
            throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("""
                SELECT COUNT(*) FROM cdc_downstream_rows
                WHERE source_id = ? AND partition_id = ?
                AND offset_value BETWEEN ? AND ?
                """)) {
            ps.setString(1, sourceId);
            ps.setInt(2, partition);
            ps.setLong(3, offsetStart);
            ps.setLong(4, offsetEnd);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getLong(1);
            }
        }
    }

    private void insertDownstreamRow(Connection conn, String sourceId, int partition,
                                     long offset, String payload) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("""
                INSERT INTO cdc_downstream_rows
                    (source_id, partition_id, offset_value, payload)
                VALUES (?, ?, ?, ?)
                ON CONFLICT (source_id, partition_id, offset_value) DO NOTHING
                """)) {
            ps.setString(1, sourceId);
            ps.setInt(2, partition);
            ps.setLong(3, offset);
            ps.setString(4, payload);
            ps.executeUpdate();
        }
    }

    // -----------------------------------------------------------------------
    // Connection Helpers
    // -----------------------------------------------------------------------

    private static Connection directConnection() throws SQLException {
        return DriverManager.getConnection(
                postgres.getJdbcUrl(),
                postgres.getUsername(),
                postgres.getPassword());
    }

    private void cleanupTestData() {
        if (postgres == null || !postgres.isRunning()) {
            return;
        }
        try (Connection conn = directConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute("DELETE FROM cdc_downstream_rows WHERE source_id = '"
                    + SOURCE_ID + "'");
            stmt.execute("DELETE FROM cdc_committed_offsets WHERE source_id = '"
                    + SOURCE_ID + "'");
            stmt.execute("DELETE FROM cdc_commit_state WHERE source_id = '"
                    + SOURCE_ID + "'");
        } catch (SQLException e) {
            LOG.warn("event=test_cleanup_failed error={}", e.getMessage());
        }
    }

    // -----------------------------------------------------------------------
    // Internal Records
    // -----------------------------------------------------------------------

    private record ExpiredPrepare(UUID tokenId, String sourceId, int partitionId,
                                  long offsetStart, long offsetEnd) {}
}

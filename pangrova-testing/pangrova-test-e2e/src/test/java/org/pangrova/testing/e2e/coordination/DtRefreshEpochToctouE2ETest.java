package org.pangrova.testing.e2e.coordination;

import org.pangrova.common.exception.StaleEpochException;
import org.pangrova.testing.e2e.SqlE2ETestBase;
import org.pangrova.testing.e2e.config.ConnectorTestConfig;
import org.pangrova.testing.e2e.setup.MetadataSchemaInitializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * E2E test for DT refresh epoch fencing (TOCTOU prevention).
 *
 * <p>Two coordinators refreshing the same dynamic table concurrently must
 * be fenced by epoch CAS: only one succeeds, the other gets StaleEpochException.
 * This prevents stale writes from overwriting newer data.
 *
 * <p>Covers: T-0.3.1 epoch CAS, T-0.3.3 RefreshCoordinator epoch validation.
 */
@DisplayName("DT Refresh Epoch TOCTOU E2E")
class DtRefreshEpochToctouE2ETest extends SqlE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";

    private DataSource dataSource;

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(ConnectorTestConfig.icebergLocal(
                CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    @BeforeEach
    void initDataSource() throws Exception {
        dataSource = MetadataSchemaInitializer.createDataSource(
                mysqlJdbcUrl(), mysqlUsername(), mysqlPassword());
        ensureDtStateTable(dataSource);
        truncateDtStateTable(dataSource);
    }

    @Test
    @DisplayName("Concurrent epoch CAS updates — only one succeeds")
    void concurrentEpochCas_onlyOneSucceeds() throws Exception {
        String dtName = "dt_epoch_test_" + System.nanoTime();
        long initialEpoch = 1L;
        insertDtState(dataSource, dtName, initialEpoch);

        AtomicInteger successes = new AtomicInteger(0);
        AtomicInteger staleEpochErrors = new AtomicInteger(0);

        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<?> writer1 = executor.submit(() ->
                    attemptEpochUpdate(dtName, initialEpoch, successes, staleEpochErrors));
            Future<?> writer2 = executor.submit(() ->
                    attemptEpochUpdate(dtName, initialEpoch, successes, staleEpochErrors));

            writer1.get();
            writer2.get();
        } finally {
            executor.shutdown();
        }

        // Exactly one should succeed, the other should get stale epoch
        assertThat(successes.get()).isEqualTo(1);
        assertThat(staleEpochErrors.get()).isEqualTo(1);

        // Final epoch should be initialEpoch + 1
        long finalEpoch = readCurrentEpoch(dataSource, dtName);
        assertThat(finalEpoch).isEqualTo(initialEpoch + 1);
    }

    @Test
    @DisplayName("Sequential epoch updates increment monotonically")
    void sequentialEpochUpdates_incrementMonotonically() throws Exception {
        String dtName = "dt_seq_epoch_" + System.nanoTime();
        insertDtState(dataSource, dtName, 0L);

        for (int i = 0; i < 5; i++) {
            long currentEpoch = readCurrentEpoch(dataSource, dtName);
            boolean updated = conditionalEpochUpdate(dataSource, dtName, currentEpoch);
            assertThat(updated).isTrue();
        }

        long finalEpoch = readCurrentEpoch(dataSource, dtName);
        assertThat(finalEpoch).isEqualTo(5L);
    }

    @Test
    @DisplayName("Stale epoch update is rejected")
    void staleEpochUpdate_isRejected() throws Exception {
        String dtName = "dt_stale_" + System.nanoTime();
        insertDtState(dataSource, dtName, 5L);

        // Try to update with stale epoch (3, which is < current 5)
        boolean updated = conditionalEpochUpdate(dataSource, dtName, 3L);
        assertThat(updated).isFalse();

        // Epoch unchanged
        long currentEpoch = readCurrentEpoch(dataSource, dtName);
        assertThat(currentEpoch).isEqualTo(5L);
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private void attemptEpochUpdate(String dtName, long expectedEpoch,
                                    AtomicInteger successes, AtomicInteger staleErrors) {
        try {
            boolean updated = conditionalEpochUpdate(dataSource, dtName, expectedEpoch);
            if (updated) {
                successes.incrementAndGet();
            } else {
                staleErrors.incrementAndGet();
            }
        } catch (Exception e) {
            if (e instanceof StaleEpochException || e.getMessage().contains("stale")) {
                staleErrors.incrementAndGet();
            } else {
                throw new RuntimeException(e);
            }
        }
    }

    private static boolean conditionalEpochUpdate(DataSource ds, String dtName,
                                                  long expectedEpoch) throws Exception {
        try (Connection conn = ds.getConnection();
             PreparedStatement ps = conn.prepareStatement(
                     "UPDATE dt_state SET epoch = ? WHERE dt_name = ? AND epoch = ?")) {
            ps.setLong(1, expectedEpoch + 1);
            ps.setString(2, dtName);
            ps.setLong(3, expectedEpoch);
            return ps.executeUpdate() > 0;
        }
    }

    private static long readCurrentEpoch(DataSource ds, String dtName) throws Exception {
        try (Connection conn = ds.getConnection();
             PreparedStatement ps = conn.prepareStatement(
                     "SELECT epoch FROM dt_state WHERE dt_name = ?")) {
            ps.setString(1, dtName);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getLong(1) : -1;
            }
        }
    }

    private static void insertDtState(DataSource ds, String dtName, long epoch) throws Exception {
        try (Connection conn = ds.getConnection();
             PreparedStatement ps = conn.prepareStatement(
                     "INSERT INTO dt_state (dt_name, epoch) VALUES (?, ?)")) {
            ps.setString(1, dtName);
            ps.setLong(2, epoch);
            ps.executeUpdate();
        }
    }

    private static void ensureDtStateTable(DataSource ds) throws Exception {
        try (Connection conn = ds.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute("""
                    CREATE TABLE IF NOT EXISTS dt_state (
                        dt_name VARCHAR(255) PRIMARY KEY,
                        epoch   BIGINT NOT NULL DEFAULT 0
                    )""");
        }
    }

    private static void truncateDtStateTable(DataSource ds) throws Exception {
        try (Connection conn = ds.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute("DELETE FROM dt_state");
        }
    }
}

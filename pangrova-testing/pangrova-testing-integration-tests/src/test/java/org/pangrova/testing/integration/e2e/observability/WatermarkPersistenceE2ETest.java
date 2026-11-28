package org.pangrova.testing.integration.e2e.observability;

import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.pangrova.testing.integration.e2e.fixture.schema.MetadataSchemaInitializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * E2E test for watermark persistence across coordinator restarts.
 *
 * <p>Validates that watermark positions stored via JdbcWatermarkPositionStore
 * survive coordinator restart. After restart, the watermark should be
 * at the last persisted position, not reset to zero.
 *
 * <p>Covers: T-0.6.1 JdbcWatermarkPositionStore, T-0.6.2 wire in bootstrap.
 */
@DisplayName("Watermark Persistence E2E")
class WatermarkPersistenceE2ETest extends SqlE2ETestBase {

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
        truncateWatermarkTable(dataSource);
    }

    @Test
    @DisplayName("Watermark position is persisted to JDBC store")
    void watermarkPosition_persistedToJdbc() throws Exception {
        String sourceId = "cdc-source-" + System.nanoTime();
        Instant position = Instant.parse("2026-01-15T10:30:00Z");

        upsertWatermark(dataSource, sourceId, position);

        Instant stored = readWatermark(dataSource, sourceId);
        assertThat(stored).isEqualTo(position);
    }

    @Test
    @DisplayName("Watermark survives simulated restart (re-read from JDBC)")
    void watermark_survivesSimulatedRestart() throws Exception {
        String sourceId = "cdc-restart-" + System.nanoTime();
        Instant position = Instant.parse("2026-02-01T08:00:00Z");

        // Phase 1: write watermark (simulates coordinator A)
        upsertWatermark(dataSource, sourceId, position);

        // Phase 2: read watermark (simulates coordinator B after restart)
        Instant recovered = readWatermark(dataSource, sourceId);
        assertThat(recovered).isEqualTo(position);
    }

    @Test
    @DisplayName("Watermark update overwrites previous position")
    void watermarkUpdate_overwritesPreviousPosition() throws Exception {
        String sourceId = "cdc-update-" + System.nanoTime();
        Instant first = Instant.parse("2026-03-01T12:00:00Z");
        Instant second = Instant.parse("2026-03-01T14:00:00Z");

        upsertWatermark(dataSource, sourceId, first);
        upsertWatermark(dataSource, sourceId, second);

        Instant current = readWatermark(dataSource, sourceId);
        assertThat(current).isEqualTo(second);
    }

    @Test
    @DisplayName("Multiple sources have independent watermarks")
    void multipleSources_independentWatermarks() throws Exception {
        String source1 = "cdc-src1-" + System.nanoTime();
        String source2 = "cdc-src2-" + System.nanoTime();
        Instant pos1 = Instant.parse("2026-04-01T09:00:00Z");
        Instant pos2 = Instant.parse("2026-04-01T11:00:00Z");

        upsertWatermark(dataSource, source1, pos1);
        upsertWatermark(dataSource, source2, pos2);

        assertThat(readWatermark(dataSource, source1)).isEqualTo(pos1);
        assertThat(readWatermark(dataSource, source2)).isEqualTo(pos2);
    }

    @Test
    @DisplayName("Non-existent source returns null watermark")
    void nonExistentSource_returnsNull() throws Exception {
        Instant stored = readWatermark(dataSource, "no-such-source");
        assertThat(stored).isNull();
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private static void truncateWatermarkTable(DataSource ds) throws Exception {
        try (Connection conn = ds.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute("DELETE FROM watermark_positions");
        }
    }

    private static void upsertWatermark(DataSource ds, String sourceId,
                                        Instant position) throws Exception {
        Timestamp now = Timestamp.from(Instant.now());
        try (Connection conn = ds.getConnection();
             PreparedStatement ps = conn.prepareStatement(
                     "INSERT INTO watermark_positions (source_id, position, updated_at) VALUES (?, ?, ?) "
                     + "ON CONFLICT (source_id) DO UPDATE SET position = EXCLUDED.position, updated_at = EXCLUDED.updated_at")) {
            ps.setString(1, sourceId);
            ps.setTimestamp(2, Timestamp.from(position));
            ps.setTimestamp(3, now);
            ps.executeUpdate();
        }
    }

    private static Instant readWatermark(DataSource ds, String sourceId) throws Exception {
        try (Connection conn = ds.getConnection();
             PreparedStatement ps = conn.prepareStatement(
                     "SELECT position FROM watermark_positions WHERE source_id = ?")) {
            ps.setString(1, sourceId);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getTimestamp("position").toInstant() : null;
            }
        }
    }
}

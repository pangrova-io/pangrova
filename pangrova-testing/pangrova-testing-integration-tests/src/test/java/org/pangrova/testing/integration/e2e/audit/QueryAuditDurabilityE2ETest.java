package org.pangrova.testing.integration.e2e.audit;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.pangrova.testing.integration.e2e.PangrovaE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.pangrova.types.value.TypedRow;

/**
 * Query audit must survive a Coordinator restart. The previous composition used
 * an in-memory Caffeine store: entries were evicted at its size cap and all
 * history was lost on restart, so audit findings could disappear silently.
 *
 * <p>This asserts the real Coordinator wrote the record to the metadata database,
 * read back through a fresh connection rather than the Coordinator's own store.</p>
 */
class QueryAuditDurabilityE2ETest extends PangrovaE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(
                ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    @Test
    void completedQueryAuditRecordIsPersistedInMetadataDatabase() throws Exception {
        List<TypedRow> rows = executeQuery("SELECT 1 AS audit_marker");
        assertThat(rows).hasSize(1);

        // Read with an independent connection: this is what a restarted
        // Coordinator (or an audit tool) would see.
        try (Connection connection = metadataDataSource().getConnection();
             PreparedStatement statement = connection.prepareStatement(
                     "SELECT success, sql_text, completed_at FROM query_audit_records "
                             + "WHERE sql_text LIKE '%audit_marker%' "
                             + "ORDER BY completed_at DESC NULLS LAST LIMIT 1");
             ResultSet result = statement.executeQuery()) {
            assertThat(result.next())
                    .as("completed query must have a durable audit record")
                    .isTrue();
            assertThat(result.getBoolean("success")).isTrue();
            assertThat(result.getString("sql_text")).contains("audit_marker");
            assertThat(result.getTimestamp("completed_at")).isNotNull();
        }
    }

    @Test
    void everyExecutedQueryWithTheMarkerGetsItsOwnRecord() throws Exception {
        executeQuery("SELECT 2 AS audit_marker_multi");
        executeQuery("SELECT 3 AS audit_marker_multi");

        try (Connection connection = metadataDataSource().getConnection();
             PreparedStatement statement = connection.prepareStatement(
                     "SELECT COUNT(*) FROM query_audit_records "
                             + "WHERE sql_text LIKE '%audit_marker_multi%'");
             ResultSet result = statement.executeQuery()) {
            assertThat(result.next()).isTrue();
            // The in-memory store's size cap could drop entries; a durable store
            // must keep one row per execution.
            assertThat(result.getLong(1)).isGreaterThanOrEqualTo(2L);
        }
    }
}
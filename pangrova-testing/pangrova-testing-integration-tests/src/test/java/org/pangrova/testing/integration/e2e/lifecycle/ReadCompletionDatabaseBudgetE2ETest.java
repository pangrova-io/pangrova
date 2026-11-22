package org.pangrova.testing.integration.e2e.lifecycle;

import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.pangrova.client.http.PangrovaRestClient;
import org.pangrova.common.client.query.QueryWaitTarget;
import org.pangrova.common.client.session.RetryPolicy;
import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;

import static org.assertj.core.api.Assertions.assertThat;

/** Measures PostgreSQL statements across the real read result and drain lifecycle. */
class ReadCompletionDatabaseBudgetE2ETest extends SqlE2ETestBase {
    private static final String JOURNAL_LOOKUP =
            "SELECT query_id, catalog_name,%FROM write_commit_journal WHERE query_id = $1";
    private static final String ANCHOR_LOOKUP =
            "SELECT owner_id, owner_incarnation, fencing_token,%FROM execution_anchor "
                    + "WHERE execution_id = $1 AND expires_at > clock_timestamp()";

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of();
    }

    @Test
    void readCompletionChecksOwnershipWithoutQueryingTheWriteJournal() throws SQLException {
        try (var connection = stateStoreDataSource().getConnection();
                var statement = connection.createStatement()) {
            statement.execute("CREATE EXTENSION IF NOT EXISTS pg_stat_statements WITH SCHEMA public");
        }
        long previousJournalLookups = calls(JOURNAL_LOOKUP);
        long previousOwnershipLookups = calls(ANCHOR_LOOKUP);
        try (var client = PangrovaRestClient.builder().coordinatorHost("localhost")
                .coordinatorPort(coordinatorHttpPort()).bearerToken(E2E_USER_BEARER_TOKEN)
                .retryPolicy(RetryPolicy.NONE).build()) {
            long deadline = System.currentTimeMillis() + Duration.ofSeconds(30).toMillis();
            var execution = client.beginQuery("SELECT CAST(42 AS BIGINT) AS answer", Map.of());
            var endpoint = execution.submission().resultEndpoint();
            while (endpoint == null) {
                var status = execution.awaitStatus(QueryWaitTarget.RESULT_OR_TERMINAL, deadline);
                assertThat(status.state()).isNotIn("FAILED", "CANCELLED");
                endpoint = status.resultEndpoint();
                if ("FINISHED".equals(status.state())) assertThat(endpoint).isNotNull();
            }
            try (var cursor = client.openResult(endpoint, deadline)) {
                assertThat(cursor.next()).isTrue();
                assertThat(((Number) cursor.value(0)).longValue()).isEqualTo(42L);
                assertThat(cursor.next()).isFalse();
            }
            var status = execution.awaitStatus(QueryWaitTarget.DRAINED, deadline);
            while (!Boolean.TRUE.equals(status.drained())) {
                assertThat(status.state()).isNotIn("FAILED", "CANCELLED");
                status = execution.awaitStatus(QueryWaitTarget.DRAINED, deadline);
            }
            assertThat(status.state()).isEqualTo("FINISHED");
        }
        assertThat(calls(JOURNAL_LOOKUP) - previousJournalLookups).as("write journal SELECTs").isZero();
        assertThat(calls(ANCHOR_LOOKUP) - previousOwnershipLookups).as("query finalization fence SELECTs").isOne();
    }

    private static long calls(String sqlPattern) throws SQLException {
        try (var connection = stateStoreDataSource().getConnection();
                var statement = connection.prepareStatement("""
                        SELECT COALESCE(SUM(calls), 0)
                        FROM public.pg_stat_statements
                        WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
                          AND query LIKE ?
                        """)) {
            statement.setString(1, sqlPattern);
            try (var rows = statement.executeQuery()) {
                assertThat(rows.next()).isTrue();
                return rows.getLong(1);
            }
        }
    }
}

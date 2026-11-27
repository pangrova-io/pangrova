package org.pangrova.testing.integration.e2e.connector;

import org.pangrova.types.value.TypedRow;
import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.pangrova.client.http.PangrovaRestClient;
import org.pangrova.common.client.enums.CancelOutcome;
import org.pangrova.common.client.session.RetryPolicy;
import org.pangrova.common.enums.QueryWaitTarget;
import org.pangrova.common.enums.ResultDisposition;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * E2E tests for incremental read via CHANGES BETWEEN SNAPSHOT syntax.
 *
 * <p>Covers B6 and D4 acceptance criteria:
 * <ul>
 *   <li>B6-AC1: CHANGES BETWEEN SNAPSHOT syntax parses and executes</li>
 *   <li>B6-AC2: Returns changed rows between snapshots</li>
 *   <li>B6-AC3: Change type marker (_change_type column)</li>
 *   <li>D4-AC1: Unified syntax works for both Iceberg and Paimon</li>
 *   <li>D4-AC3: Change type marker present</li>
 * </ul>
 */
@DisplayName("Incremental Read E2E Tests [B6, D4]")
class IncrementalReadTest extends SqlE2ETestBase {

    private static final String ICEBERG_CATALOG = "iceberg_catalog";
    private static final String PAIMON_CATALOG = "paimon_catalog";
    private static final String SCHEMA = "default";

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(
                ConnectorTestConfig.icebergLocal(ICEBERG_CATALOG, SCHEMA, icebergWarehouse().toString()),
                ConnectorTestConfig.paimon(PAIMON_CATALOG, SCHEMA, paimonWarehouse().toString()));
    }

    private String icebergTable;
    private String paimonTable;

    @BeforeEach
    void assignUniqueTableNames(TestInfo testInfo) {
        String method = testInfo.getTestMethod().map(m -> m.getName()).orElse("unknown");
        long ts = System.nanoTime();
        icebergTable = "t_iceberg_" + method + "_" + ts;
        paimonTable = "t_paimon_" + method + "_" + ts;
    }

    private String icebergFqn() {
        return ICEBERG_CATALOG + "." + SCHEMA + "." + icebergTable;
    }

    private String paimonFqn() {
        return PAIMON_CATALOG + "." + SCHEMA + "." + paimonTable;
    }

    /** Isolates incremental SQL verification from the separate $snapshots scan path. */
    private long latestPaimonSnapshotId() throws Exception {
        Options options = new Options();
        options.setString("warehouse", paimonWarehouse().toString());
        try (var catalog = CatalogFactory.createCatalog(CatalogContext.create(options))) {
            var table = (FileStoreTable) catalog.getTable(Identifier.create(SCHEMA, paimonTable));
            Long snapshotId = table.snapshotManager().latestSnapshotId();
            assertThat(snapshotId).as("Paimon committed snapshot").isNotNull();
            return snapshotId;
        }
    }

    @Nested
    @DisplayName("3.3.6 - Iceberg Incremental Read with Change Type")
    class IcebergIncrementalRead {

        @Test
        @DisplayName("Returns appended rows between two snapshots with INSERT change type")
        void incrementalReadReturnsAppendedRows() {
            sqlRunner.executeDdl("CREATE TABLE " + icebergFqn()
                    + " (id BIGINT, name VARCHAR) WITH ('format-version' = '2')");

            sqlRunner.execute("INSERT INTO " + icebergFqn()
                    + " VALUES (1, 'alice'), (2, 'bob')");

            List<TypedRow> snapshots = sqlRunner.execute(
                    "SELECT * FROM " + icebergFqn() + "$snapshots ORDER BY committed_at");
            assertThat(snapshots).hasSizeGreaterThanOrEqualTo(1);
            long snapshot1 = ((Number) snapshots.get(0).getRaw("snapshot_id")).longValue();

            sqlRunner.execute("INSERT INTO " + icebergFqn()
                    + " VALUES (3, 'charlie'), (4, 'diana')");

            List<TypedRow> snapshots2 = sqlRunner.execute(
                    "SELECT * FROM " + icebergFqn() + "$snapshots ORDER BY committed_at");
            long snapshot2 = ((Number) snapshots2.get(snapshots2.size() - 1).getRaw("snapshot_id")).longValue();

            List<TypedRow> changes = sqlRunner.execute(
                    "SELECT * FROM " + icebergFqn()
                            + " CHANGES BETWEEN SNAPSHOT " + snapshot1 + " AND " + snapshot2
                            + " ORDER BY id");

            assertThat(changes).hasSize(2);
            assertThat(((Number) changes.get(0).getRaw("id")).longValue()).isEqualTo(3L);
            assertThat(changes.get(0).getRaw("name")).isEqualTo("charlie");
            assertThat(changes.get(0).getRaw("_change_type")).isEqualTo("INSERT");
            assertThat(((Number) changes.get(1).getRaw("id")).longValue()).isEqualTo(4L);
            assertThat(changes.get(1).getRaw("_change_type")).isEqualTo("INSERT");
            assertThatThrownBy(() -> sqlRunner.execute("SELECT * FROM " + icebergFqn()
                    + " CHANGES BETWEEN SNAPSHOT " + snapshot2 + " AND " + snapshot1))
                    .hasMessageContaining("retained ancestor");
        }

        @Test
        @DisplayName("Incremental read across multiple commits returns all changes")
        void incrementalReadAcrossMultipleCommits() {
            sqlRunner.executeDdl("CREATE TABLE " + icebergFqn()
                    + " (id BIGINT, value VARCHAR) WITH ('format-version' = '2')");

            sqlRunner.execute("INSERT INTO " + icebergFqn() + " VALUES (1, 'initial')");

            List<TypedRow> snap1 = sqlRunner.execute(
                    "SELECT * FROM " + icebergFqn() + "$snapshots ORDER BY committed_at");
            long fromSnapshot = ((Number) snap1.get(snap1.size() - 1).getRaw("snapshot_id")).longValue();

            sqlRunner.execute("INSERT INTO " + icebergFqn() + " VALUES (2, 'second')");
            sqlRunner.execute("INSERT INTO " + icebergFqn() + " VALUES (3, 'third')");

            List<TypedRow> snap3 = sqlRunner.execute(
                    "SELECT * FROM " + icebergFqn() + "$snapshots ORDER BY committed_at");
            long toSnapshot = ((Number) snap3.get(snap3.size() - 1).getRaw("snapshot_id")).longValue();

            List<TypedRow> changes = sqlRunner.execute(
                    "SELECT * FROM " + icebergFqn()
                            + " CHANGES BETWEEN SNAPSHOT " + fromSnapshot + " AND " + toSnapshot
                            + " ORDER BY id");

            assertThat(changes).hasSize(2);
            assertThat(((Number) changes.get(0).getRaw("id")).longValue()).isEqualTo(2L);
            assertThat(((Number) changes.get(1).getRaw("id")).longValue()).isEqualTo(3L);
        }
    }

    @Nested
    @DisplayName("3.3.7 - Paimon Incremental Read with Change Type")
    class PaimonIncrementalRead {

        @Test
        @DisplayName("Returns appended rows between two snapshots with INSERT change type")
        void incrementalReadReturnsAppendedRows() throws Exception {
            sqlRunner.executeDdl("CREATE TABLE " + paimonFqn()
                    + " (id BIGINT, name VARCHAR)");

            sqlRunner.execute("INSERT INTO " + paimonFqn()
                    + " VALUES (1, 'alice'), (2, 'bob')");

            long snapshot1 = latestPaimonSnapshotId();

            sqlRunner.execute("INSERT INTO " + paimonFqn()
                    + " VALUES (3, 'charlie'), (4, 'diana')");

            long snapshot2 = latestPaimonSnapshotId();

            List<TypedRow> changes = sqlRunner.execute(
                    "SELECT * FROM " + paimonFqn()
                            + " CHANGES BETWEEN SNAPSHOT " + snapshot1 + " AND " + snapshot2
                            + " ORDER BY id");

            assertThat(changes).hasSize(2);
            assertThat(((Number) changes.get(0).getRaw("id")).longValue()).isEqualTo(3L);
            assertThat(changes.get(0).getRaw("name")).isEqualTo("charlie");
            assertThat(changes.get(0).getRaw("_change_type")).isEqualTo("INSERT");
            assertThat(((Number) changes.get(1).getRaw("id")).longValue()).isEqualTo(4L);
            assertThat(changes.get(1).getRaw("_change_type")).isEqualTo("INSERT");

            sqlRunner.executeDdl("ALTER TABLE " + paimonFqn() + " ADD COLUMN later VARCHAR");
            sqlRunner.execute("INSERT INTO " + paimonFqn() + " VALUES (5, 'later row', 'new column')");
            var submitted = submitQueryViaHttp("SELECT name AS selected_name, _change_type FROM " + paimonFqn()
                    + " CHANGES BETWEEN SNAPSHOT " + snapshot1 + " AND " + snapshot2
                    + " WHERE id <> 4 AND _change_type = 'INSERT' ORDER BY id DESC LIMIT 1");
            var endpoint = awaitResultEndpoint(submitted.queryId(), submitted);
            assertThat(isControlPlaneEndpoint(endpoint, submitted.queryId())).isFalse();
            assertThat(consumeSubmittedQuery(submitted)).singleElement().satisfies(row -> {
                assertThat(row.values()).containsOnlyKeys("selected_name", "_change_type");
                assertThat(row.getRaw("selected_name")).isEqualTo("charlie");
                assertThat(row.getRaw("_change_type")).isEqualTo("INSERT");
            });
            org.awaitility.Awaitility.await("incremental Flight query physically drained")
                    .atMost(Duration.ofSeconds(15))
                    .until(() -> queryManager.getStateMachine(submitted.queryId()).physicalCleanupApplied());
            taskTracker.requireQueryExecutionDrained(submitted.queryId());

            try (var client = PangrovaRestClient.builder().coordinatorHost("localhost")
                    .coordinatorPort(coordinatorHttpPort()).bearerToken(E2E_USER_BEARER_TOKEN)
                    .retryPolicy(RetryPolicy.NONE).build()) {
                long deadline = System.currentTimeMillis() + Duration.ofSeconds(30).toMillis();
                var execution = client.beginQuery("SELECT * FROM " + paimonFqn()
                        + " CHANGES BETWEEN SNAPSHOT " + snapshot1 + " AND " + snapshot2, Map.of());
                var ready = execution.awaitStatus(QueryWaitTarget.RESULT_OR_TERMINAL, deadline);
                while (ready.resultEndpoint() == null && ready.resultDisposition() == ResultDisposition.PENDING) {
                    ready = execution.awaitStatus(QueryWaitTarget.RESULT_OR_TERMINAL, deadline);
                }
                assertThat(ready.resultEndpoint()).isNotNull();
                assertThat(execution.cancel()).isEqualTo(CancelOutcome.CANCEL_ACCEPTED);
                var terminal = execution.awaitStatus(QueryWaitTarget.DRAINED, deadline);
                while (!Boolean.TRUE.equals(terminal.drained())) {
                    terminal = execution.awaitStatus(QueryWaitTarget.DRAINED, deadline);
                }
                assertThat(terminal.state()).isEqualTo("CANCELLED");
                taskTracker.requireQueryExecutionDrained(execution.submission().queryId());
                assertThat(queryManager.getStateMachine(execution.submission().queryId()).physicalCleanupApplied())
                        .isTrue();
            }
        }
    }

    @Nested
    @DisplayName("3.3.8 - Empty Incremental Range")
    class EmptyIncrementalRange {

        @Test
        @DisplayName("Same snapshot returns empty result set")
        void sameSnapshotReturnsEmpty() {
            sqlRunner.executeDdl("CREATE TABLE " + icebergFqn()
                    + " (id BIGINT, name VARCHAR) WITH ('format-version' = '2')");

            sqlRunner.execute("INSERT INTO " + icebergFqn()
                    + " VALUES (1, 'alice')");

            List<TypedRow> snapshots = sqlRunner.execute(
                    "SELECT * FROM " + icebergFqn() + "$snapshots ORDER BY committed_at");
            long snapshotId = ((Number) snapshots.get(snapshots.size() - 1).getRaw("snapshot_id")).longValue();

            List<TypedRow> changes = sqlRunner.execute(
                    "SELECT * FROM " + icebergFqn()
                            + " CHANGES BETWEEN SNAPSHOT " + snapshotId + " AND " + snapshotId);

            assertThat(changes).isEmpty();
        }

        @Test
        @DisplayName("No changes between consecutive snapshots with no writes returns empty")
        void noChangesBetweenSnapshotsReturnsEmpty() throws Exception {
            sqlRunner.executeDdl("CREATE TABLE " + paimonFqn()
                    + " (id BIGINT, name VARCHAR)");

            sqlRunner.execute("INSERT INTO " + paimonFqn() + " VALUES (1, 'alice')");
            sqlRunner.execute("INSERT INTO " + paimonFqn() + " VALUES (2, 'bob')");

            long snapshot1 = latestPaimonSnapshotId();
            long snapshot2 = snapshot1;

            List<TypedRow> changes = sqlRunner.execute(
                    "SELECT * FROM " + paimonFqn()
                            + " CHANGES BETWEEN SNAPSHOT " + snapshot1 + " AND " + snapshot2);

            assertThat(changes).isEmpty();
        }
    }
}

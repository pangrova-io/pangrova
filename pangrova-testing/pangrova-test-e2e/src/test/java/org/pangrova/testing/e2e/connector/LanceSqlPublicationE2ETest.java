package org.pangrova.testing.e2e.connector;

import org.junit.jupiter.api.Test;
import org.pangrova.connector.lance.LanceTableHandle;
import org.pangrova.metadata.coordination.JdbcWriteCommitJournal;
import org.pangrova.protocol.task.CanonicalTaskRequests;
import org.pangrova.protocol.v2.WriteOutputBinding;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;
import org.pangrova.spi.connector.ConnectorTransactionHandle;
import org.pangrova.spi.connector.metadata.Constraint;
import org.pangrova.spi.connector.metadata.DynamicFilter;
import org.pangrova.spi.connector.metadata.table.ConnectorReadBinding;
import org.pangrova.spi.coordination.WriteCommitEntry;
import org.pangrova.testing.E2ETest;
import org.pangrova.testing.e2e.PangrovaE2ETestBase;
import org.pangrova.testing.e2e.config.ConnectorTestConfig;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Public SQL, real Worker/JNI dataset writes and journaled manifest publication. */
@E2ETest
class LanceSqlPublicationE2ETest extends PangrovaE2ETestBase {
    private static final String CATALOG = "lance_sql_publication";
    private static final String SCHEMA = "default";

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(new ConnectorTestConfig(CATALOG, SCHEMA, "lance", Map.of(
                "warehouse", tempDir.resolve("lance-sql-warehouse").toString(),
                "jdbc.url", pgJdbcUrl(), "jdbc.user", pgUsername(), "jdbc.password", pgPassword())));
    }

    @Test
    void createInsertSelectPublishesOneRealManifestAndDurableIntent() throws Exception {
        String table = uniqueTable();
        String name = CATALOG + "." + SCHEMA + "." + table;
        executeDdl("CREATE TABLE " + name + " (id BIGINT, name VARCHAR(40), score DOUBLE)");
        var response = submitQueryViaHttp("INSERT INTO " + name
                + " VALUES (1, 'alice', 1.25), (2, 'bob', 2.5)");
        assertThat(pollUntilTerminal(response.queryId())).contains("\"FINISHED\"");
        var rows = executeQuery("SELECT id, name, score FROM " + name + " ORDER BY id");
        assertThat(rows).extracting(row -> List.of(row.getLong("id"), row.getString("name"), row.getDouble("score")))
                .containsExactly(List.of(1L, "alice", 1.25), List.of(2L, "bob", 2.5));
        var journal = new JdbcWriteCommitJournal(stateStoreDataSource());
        var intent = journal.getEntry(response.queryId()).orElseThrow();
        assertThat(intent.state()).isEqualTo(WriteCommitEntry.State.COMMITTED);
        assertThat(intent.publication()).isNotNull();
        assertThat(intent.publication().publishedVersion()).isEqualTo(1);
        assertThat(intent.executionFencingToken()).isPositive();
        assertThat(WriteOutputBinding.parseFrom(intent.writeOutputBinding()).hasLance()).isTrue();
        LanceTableHandle pinned = table(table);
        assertThat(pinned.version()).isEqualTo(1);
        byte[] manifest = firstManifest(pinned);
        assertThat(intent.publication().manifestSha256())
                .containsExactly(CanonicalTaskRequests.sha256(manifest).getDigest().toByteArray());
        assertThat(pinned.rowCount()).isEqualTo(2);
    }

    @Test
    void selectedEmptySnapshotStaysEmptyAfterFirstPublicSqlPublication() {
        String table = uniqueTable();
        String name = CATALOG + "." + SCHEMA + "." + table;
        executeDdl("CREATE TABLE " + name + " (id BIGINT)");
        var session = authenticatedCatalogSession(CATALOG);
        var catalog = metadataBootstrap.catalogRegistry().getCatalog(CATALOG);
        var metadata = catalog.getMetadata(session);
        var selected = metadata.bindRead(session, metadata.getTableHandle(session, new SchemaTableName(SCHEMA, table)));
        assertThat(selected.state()).isEqualTo(ConnectorReadBinding.SnapshotState.EMPTY);
        executeDml("INSERT INTO " + name + " VALUES (7)");
        assertThat(executeQuery("SELECT id FROM " + name)).extracting(row -> row.getLong("id"))
                .containsExactly(7L);
        assertThat(table(table).version()).isEqualTo(1);
        metadata.validateReadBinding(session, selected);
        var splits = catalog.getSplitManager().getSplits(ConnectorTransactionHandle.EMPTY, session,
                selected.handle(), DynamicFilter.ALL, Constraint.EMPTY);
        try {
            assertThat(splits.isFinished()).isTrue();
            assertThat(splits.getNextBatch(1)).isEmpty();
            assertThat(metadata.getTableStatistics(session, selected.handle()).rowCount()).isZero();
        } finally {
            splits.close();
        }
        executeDdl("DROP TABLE " + name);
        executeDdl("CREATE TABLE " + name + " (id BIGINT)");
        assertThatThrownBy(() -> catalog.getSplitManager().getSplits(ConnectorTransactionHandle.EMPTY, session,
                selected.handle(), DynamicFilter.ALL, Constraint.EMPTY))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("incarnation");
    }

    @Test
    void appendAndOverwriteKeepPinnedOldHandleAndReplaceVisibleRows() {
        String table = uniqueTable();
        String name = CATALOG + "." + SCHEMA + "." + table;
        executeDdl("CREATE TABLE " + name + " (id BIGINT, name VARCHAR(40))");
        executeDml("INSERT INTO " + name + " VALUES (1, 'first')");
        LanceTableHandle old = table(table);
        executeDml("INSERT INTO " + name + " VALUES (2, 'second')");
        assertThat(executeQuery("SELECT id FROM " + name + " ORDER BY id").stream().map(row -> row.getLong("id")))
                .containsExactly(1L, 2L);
        assertThat(table(table).version()).isEqualTo(2);
        executeDml("INSERT OVERWRITE " + name + " VALUES (9, 'replacement')");
        assertThat(executeQuery("SELECT id, name FROM " + name).stream()
                .map(row -> List.of(row.getLong("id"), row.getString("name"))))
                .containsExactly(List.of(9L, "replacement"));
        assertThat(table(table).version()).isEqualTo(3);
        assertThat(table(table).rowCount()).isEqualTo(1);
        assertThat(old.version()).isEqualTo(1);
    }

    private static LanceTableHandle table(String name) {
        var session = authenticatedCatalogSession(CATALOG);
        var metadata = metadataBootstrap.catalogRegistry().getCatalog(CATALOG).getMetadata(session);
        return (LanceTableHandle) metadata.getTableHandle(session, new SchemaTableName(SCHEMA, name));
    }

    private static byte[] firstManifest(LanceTableHandle table) throws java.io.IOException {
        try (var paths = Files.list(Path.of(table.datasetUri()).resolve("_versions"))) {
            var manifests = paths.filter(path -> path.getFileName().toString().endsWith(".manifest")).toList();
            assertThat(manifests).hasSize(1);
            return Files.readAllBytes(manifests.get(0));
        }
    }

    private static String uniqueTable() { return "sql_" + UUID.randomUUID().toString().replace("-", ""); }
}

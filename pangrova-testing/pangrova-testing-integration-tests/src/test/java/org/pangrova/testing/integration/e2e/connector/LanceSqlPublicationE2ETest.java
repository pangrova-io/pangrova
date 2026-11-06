package org.pangrova.testing.integration.e2e.connector;

import org.pangrova.testing.integration.control.ai.ModelAuthorityFixtures;

import org.pangrova.protocol.checksum.ProtocolChecksums;

import org.junit.jupiter.api.Test;
import org.pangrova.connector.lance.LanceTableHandle;
import org.pangrova.persistence.query.postgresql.transaction.JdbcWriteCommitJournal;
import org.pangrova.protocol.v2.WriteOutputBinding;
import org.pangrova.connector.api.metadata.table.SchemaTableName;
import org.pangrova.connector.api.transaction.ConnectorTransactionHandle;
import org.pangrova.connector.api.metadata.Constraint;
import org.pangrova.connector.api.metadata.DynamicFilter;
import org.pangrova.connector.api.metadata.table.ConnectorReadBinding;
import org.pangrova.persistence.query.api.commit.WriteCommitEntry;
import org.pangrova.testing.testkit.E2ETest;
import org.pangrova.testing.integration.e2e.PangrovaE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
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
                "warehouse", tempDir.resolve("lance-sql-warehouse").toString())));
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
                .containsExactly(ProtocolChecksums.sha256(manifest).getDigest().toByteArray());
        assertThat(pinned.rowCount()).isEqualTo(2);
        var accepted = snapshot(pinned);
        assertThat(accepted.version()).isEqualTo(intent.publication().publishedVersion());
        assertThat(accepted.membershipId()).isEqualTo(intent.commitFingerprint());
        assertThat(accepted.manifestSha256()).isEqualTo(intent.publication().manifestSha256());
        assertThat(accepted.schemaSha256()).isEqualTo(pinned.schemaFence());
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
        var splits = catalog.getSplitManager().getSplits(ConnectorTransactionHandle.empty(), session,
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
        assertThatThrownBy(() -> catalog.getSplitManager().getSplits(ConnectorTransactionHandle.empty(), session,
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
        assertThat(executeQuery("SELECT id FROM " + name + " FOR VERSION AS OF 1 ORDER BY id"))
                .extracting(row -> row.getLong("id")).containsExactly(1L);
        assertThat(executeQuery("SELECT id FROM " + name + " FOR VERSION AS OF 2 ORDER BY id"))
                .extracting(row -> row.getLong("id")).containsExactly(1L, 2L);
        var historical = snapshot(old);
        var current = snapshot(table(table));
        assertThat(historical.version()).isEqualTo(1);
        assertThat(current.version()).isEqualTo(3);
        assertThat(historical.membershipId()).isNotEqualTo(current.membershipId());
        assertThat(historical.tableId()).isEqualTo(current.tableId());
        assertThat(historical.generation()).isEqualTo(current.generation());
    }

    private static LanceTableHandle table(String name) {
        var session = authenticatedCatalogSession(CATALOG);
        var metadata = metadataBootstrap.catalogRegistry().getCatalog(CATALOG).getMetadata(session);
        return (LanceTableHandle) metadata.getTableHandle(session, new SchemaTableName(SCHEMA, name));
    }

    private static byte[] firstManifest(LanceTableHandle table) throws java.io.IOException {
        var accepted = snapshot(table);
        assertThat(Files.exists(Path.of(table.datasetUri()).resolve("_versions"))).isFalse();
        Path actual = Path.of(accepted.datasetUri()).resolve(accepted.manifestRelativePath());
        try (var paths = Files.list(actual.getParent())) {
            var manifests = paths.filter(path -> path.getFileName().toString().endsWith(".manifest")).toList();
            assertThat(manifests).hasSize(1);
            assertThat(manifests).containsExactly(actual);
            byte[] bytes = Files.readAllBytes(actual);
            assertThat(bytes.length).isEqualTo(accepted.manifestSize());
            assertThat(ProtocolChecksums.sha256(bytes).getDigest().toByteArray()).isEqualTo(accepted.manifestSha256());
            return bytes;
        }
    }

    private static org.pangrova.connector.api.metadata.AcceptedSourceSnapshot snapshot(LanceTableHandle table) {
        var descriptor = metadataBootstrap.catalogRegistry().getCatalog(CATALOG).getDescriptor();
        var authority = new org.pangrova.metadata.source.jdbc.JdbcManagedSourceCatalog(stateStoreDataSource(),
                new org.pangrova.connector.api.metadata.SourceCatalogDeployment(
                        descriptor.name(), descriptor.incarnationId(), descriptor.revision()),
                execution -> e2eCoordinationForTest().requireExecutionAnchor(execution),
                new org.pangrova.persistence.query.postgresql.coordination.JdbcExecutionTransactionFence(),
                ModelAuthorityFixtures.models());
        var key = new org.pangrova.connector.api.metadata.SourceTableKey(table.tenantId(), table.table());
        var actual = authority.load(key).orElseThrow();
        assertThat(actual.tableId()).isEqualTo(table.tableId());
        var selector = new org.pangrova.connector.api.metadata.SourceTableState(actual.key(), actual.tableId(),
                actual.datasetUri(), actual.generation(), actual.revision(), table.version(), actual.rows(),
                actual.schemaSha256(), actual.payload());
        return authority.snapshot(selector).reference();
    }

    private static String uniqueTable() { return "sql_" + UUID.randomUUID().toString().replace("-", ""); }
}

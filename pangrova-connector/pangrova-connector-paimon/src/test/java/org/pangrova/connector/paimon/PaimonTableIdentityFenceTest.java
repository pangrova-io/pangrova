package org.pangrova.connector.paimon;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.CommitMessageSerializer;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Test;
import org.pangrova.connector.paimon.handle.PaimonTableIdentity;
import org.pangrova.connector.paimon.write.PaimonPageSinkProvider;
import org.pangrova.spi.connector.ConnectorTransactionHandle;
import org.pangrova.spi.connector.data.ConnectorPageSink;
import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.spi.connector.data.page.Page;
import org.pangrova.spi.connector.metadata.ConnectorOutputTableHandle;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;
import org.pangrova.testfixtures.tableformat.PaimonCommitTestSupport;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Fences Paimon writes and replay against a same-name table that was dropped and recreated. */
class PaimonTableIdentityFenceTest extends PaimonDeleteTestSupport {

    private static final StoredRow ROW = new StoredRow(1, "east", "one", "value");
    private static final List<String> COLUMNS = List.of("id", "region", "day", "payload");

    @Test
    void pageSinkRejectsTableRecreatedAfterPlanning() throws Exception {
        createTable(false, false);
        var metadata = metadata();
        ConnectorOutputTableHandle output = metadata.beginInsert(SESSION, handle(metadata));

        recreateTable();

        assertThatThrownBy(() -> provider().createPageSink(
                ConnectorTransactionHandle.EMPTY, SESSION, output))
                .hasMessageContaining("identity changed");
    }

    @Test
    void commitReplayRejectsTableRecreatedAfterPlanning() throws Exception {
        createTable(false, false);
        var metadata = metadata();
        ConnectorOutputTableHandle output = metadata.beginInsert(SESSION, handle(metadata));
        byte[] fragment = prepare(provider().createPageSink(
                ConnectorTransactionHandle.EMPTY, SESSION, output), ROW);
        var context = PaimonCommitTestSupport.context(metadata, SESSION, output, List.of(fragment));

        recreateTable();

        assertThatThrownBy(() -> metadata.finishDistributedWrite(
                SESSION, output, List.of(fragment), context))
                .hasMessageContaining("identity changed");
        assertThat(readRows()).isEmpty();
    }

    @Test
    void schemaChangeAfterPlanningFailsClosed() throws Exception {
        createTable(false, false);
        var metadata = metadata();
        ConnectorOutputTableHandle output = metadata.beginInsert(SESSION, handle(metadata));

        catalog.alterTable(IDENTIFIER, SchemaChange.addColumn("extra", DataTypes.STRING()), false);

        assertThatThrownBy(() -> provider().createPageSink(
                ConnectorTransactionHandle.EMPTY, SESSION, output))
                .hasMessageContaining("schema changed");
    }

    @Test
    void abortDeletesOnlyUnpublishedStagedFiles() throws Exception {
        createTable(false, false);
        var metadata = metadata();
        ConnectorOutputTableHandle output = metadata.beginInsert(SESSION, handle(metadata));
        byte[] fragment = prepare(provider().createPageSink(
                ConnectorTransactionHandle.EMPTY, SESSION, output), ROW);
        List<Path> staged = stagedDataFiles((FileStoreTable) freshTable(), fragment);
        assertThat(staged).isNotEmpty().allMatch(Files::exists);

        metadata.abortDistributedWrite(SESSION, output, List.of(fragment));

        assertThat(staged).allMatch(path -> !Files.exists(path));
        assertThat(readRows()).isEmpty();
    }

    @Test
    void abortKeepsStagedFilesWhenAnotherSnapshotAdvanced() throws Exception {
        createTable(false, false);
        var metadata = metadata();
        ConnectorOutputTableHandle output = metadata.beginInsert(SESSION, handle(metadata));
        byte[] fragment = prepare(provider().createPageSink(
                ConnectorTransactionHandle.EMPTY, SESSION, output), ROW);
        List<Path> staged = stagedDataFiles((FileStoreTable) freshTable(), fragment);
        writeRows(new StoredRow(2, "west", "two", "committed"));

        metadata.abortDistributedWrite(SESSION, output, List.of(fragment));

        assertThat(staged).allMatch(Files::exists);
        assertThat(readRows()).containsExactly(new StoredRow(2, "west", "two", "committed"));
    }

    @Test
    void schemaFingerprintChangesOnlyWithTheDeclaredSchema() throws Exception {
        FileStoreTable table = (FileStoreTable) createTable(false, false);
        String original = PaimonTableIdentity.schemaFingerprint(table.schema());
        assertThat(PaimonTableIdentity.schemaFingerprint(table.schema())).isEqualTo(original);

        catalog.alterTable(IDENTIFIER, SchemaChange.addColumn("extra", DataTypes.STRING()), false);

        assertThat(PaimonTableIdentity.schemaFingerprint(
                ((FileStoreTable) freshTable()).schema())).isNotEqualTo(original);
    }

    private PaimonMetadata metadata() {
        return new PaimonMetadata(catalog, "delete_catalog", new PaimonSplitRegistry());
    }

    private void recreateTable() throws Exception {
        catalog.dropTable(IDENTIFIER, false);
        createTable(false, false);
    }

    private org.pangrova.spi.connector.metadata.table.ConnectorTableHandle handle(PaimonMetadata metadata) {
        return metadata.getTableHandle(SESSION, new SchemaTableName(DATABASE, TABLE_NAME));
    }

    private PaimonPageSinkProvider provider() {
        return new PaimonPageSinkProvider(catalog, warehouse.resolve("spill").toString());
    }

    private static byte[] prepare(ConnectorPageSink sink, StoredRow... rows) {
        try {
            sink.appendPage(new Page(COLUMNS, Arrays.stream(rows).map(row -> TypedRow.builder()
                    .putLong("id", row.id()).putString("region", row.region())
                    .putString("day", row.day()).putString("payload", row.payload()).build()).toList()));
            return sink.finishWithMetadata();
        } catch (RuntimeException | Error failure) {
            sink.abort();
            throw failure;
        } finally {
            sink.close();
        }
    }

    private static List<Path> stagedDataFiles(FileStoreTable table, byte[] fragment) throws Exception {
        var serializer = new CommitMessageSerializer();
        List<Path> paths = new ArrayList<>();
        try (var in = new DataInputStream(new ByteArrayInputStream(fragment))) {
            int version = in.readInt();
            in.readUTF();
            in.readUTF();
            in.readUTF();
            int count = in.readInt();
            for (int i = 0; i < count; i++) {
                byte[] bytes = new byte[in.readInt()];
                in.readFully(bytes);
                var message = (CommitMessageImpl) serializer.deserialize(version, bytes);
                var factory = table.store().pathFactory()
                        .createDataFilePathFactory(message.partition(), message.bucket());
                message.newFilesIncrement().newFiles()
                        .forEach(file -> paths.add(Paths.get(factory.toPath(file).toUri())));
            }
        }
        return List.copyOf(paths);
    }
}

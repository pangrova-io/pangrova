package org.pangrova.connector.paimon.system;

import org.pangrova.spi.connector.data.value.TypedRow;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for Paimon system tables using a real filesystem Catalog.
 * Validates $snapshots and $options system tables return correct metadata
 * after data is written to a real Paimon table.
 */
class PaimonSystemTableTest {

    @TempDir
    static Path warehouse;

    private static Table testTable;

    @BeforeAll
    static void setUp() throws Exception {
        Catalog catalog = buildCatalog();
        catalog.createDatabase("test_db", false);

        Schema schema = Schema.newBuilder()
                .column("id", DataTypes.BIGINT())
                .column("name", DataTypes.STRING())
                .primaryKey("id")
                .option("bucket", "1")
                .build();
        catalog.createTable(Identifier.create("test_db", "orders"), schema, false);
        testTable = catalog.getTable(Identifier.create("test_db", "orders"));
        writeTestData(testTable);
    }

    @Test
    void snapshotsSystemTable_returnsSnapshotAfterWrite() {
        PaimonSnapshotsSystemTable systemTable = new PaimonSnapshotsSystemTable(testTable);

        assertThat(systemTable.columns()).containsExactly(
                "snapshot_id", "schema_id", "commit_kind", "commit_time", "total_record_count");

        List<TypedRow> rows = systemTable.rows();
        assertThat(rows).isNotEmpty();

        TypedRow first = rows.get(0);
        assertThat(first.getLong("snapshot_id")).isEqualTo(1L);
        assertThat(first.getLong("schema_id")).isEqualTo(0L);
        assertThat(first.getString("commit_kind")).isNotBlank();
        assertThat(first.getLong("commit_time")).isGreaterThan(0);
        assertThat(first.getLong("total_record_count")).isGreaterThan(0);
    }

    @Test
    void optionsSystemTable_returnsTableOptions() {
        PaimonOptionsSystemTable systemTable = new PaimonOptionsSystemTable(testTable);

        assertThat(systemTable.columns()).containsExactly("key", "value");

        List<TypedRow> rows = systemTable.rows();
        assertThat(rows).isNotEmpty();

        boolean hasBucket = rows.stream()
                .anyMatch(r -> "bucket".equals(r.getString("key"))
                        && "1".equals(r.getString("value")));
        assertThat(hasBucket).isTrue();
    }

    @Test
    void schemasSystemTable_returnsSchemaHistory() {
        PaimonSchemasSystemTable systemTable = new PaimonSchemasSystemTable(testTable);

        assertThat(systemTable.columns()).containsExactly(
                "schema_id", "fields", "partition_keys", "primary_keys");

        List<TypedRow> rows = systemTable.rows();
        assertThat(rows).isNotEmpty();

        TypedRow first = rows.get(0);
        assertThat(first.getLong("schema_id")).isEqualTo(0L);
        assertThat(first.getString("fields")).contains("id");
        assertThat(first.getString("primary_keys")).contains("id");
    }

    @Test
    void snapshotsSystemTable_emptyTableReturnsNoRows() throws Exception {
        Catalog catalog = buildCatalog();
        try { catalog.createDatabase("empty_db", true); } catch (Exception ignored) {}

        Schema schema = Schema.newBuilder()
                .column("id", DataTypes.BIGINT())
                .primaryKey("id")
                .option("bucket", "1")
                .build();
        Identifier emptyId = Identifier.create("empty_db", "empty_table");
        catalog.createTable(emptyId, schema, true);
        Table emptyTable = catalog.getTable(emptyId);

        PaimonSnapshotsSystemTable systemTable = new PaimonSnapshotsSystemTable(emptyTable);
        assertThat(systemTable.rows()).isEmpty();
    }

    private static Catalog buildCatalog() {
        Options options = new Options();
        options.set("warehouse", warehouse.toString());
        CatalogContext context = CatalogContext.create(options);
        return CatalogFactory.createCatalog(context);
    }

    private static void writeTestData(Table table) throws Exception {
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        BatchTableWrite write = writeBuilder.newWrite();
        write.write(GenericRow.of(1L, BinaryString.fromString("alice")));
        write.write(GenericRow.of(2L, BinaryString.fromString("bob")));
        write.write(GenericRow.of(3L, BinaryString.fromString("charlie")));
        List<CommitMessage> messages = write.prepareCommit();
        write.close();

        BatchTableCommit commit = writeBuilder.newCommit();
        commit.commit(messages);
        commit.close();
    }
}

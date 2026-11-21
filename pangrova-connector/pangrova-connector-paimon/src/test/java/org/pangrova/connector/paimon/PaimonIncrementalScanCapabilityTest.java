package org.pangrova.connector.paimon;

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
import org.apache.paimon.types.RowKind;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Integration test for {@link PaimonIncrementalScanCapability} using
 * real Paimon FileSystemCatalog backed by a temporary directory.
 *
 * <p>Paimon snapshot IDs are sequential (1, 2, 3, ...), so snapshot
 * ordering is deterministic. Tests still verify actual snapshot existence.</p>
 */
class PaimonIncrementalScanCapabilityTest {

    @TempDir
    static Path warehouse;

    private static Catalog catalog;
    private static PaimonIncrementalScanCapability capability;

    private static final String DATABASE = "test_db";

    @BeforeAll
    static void setUp() throws Exception {
        catalog = buildCatalog();
        catalog.createDatabase(DATABASE, false);
        capability = new PaimonIncrementalScanCapability(catalog);
    }

    @Test
    void supportsIncrementalAppendScan_returnsTrue() {
        assertThat(capability.supportsIncrementalAppendScan()).isTrue();
    }

    @Test
    void supportsChangelogScan_returnsTrue() {
        assertThat(capability.supportsChangelogScan()).isTrue();
    }

    /**
     * Append-only table (no primary key): all commits are APPEND kind.
     * Insert rows in two separate commits, verify range is append-only.
     */
    @Test
    void isAppendOnly_appendOnlyRange_returnsTrue() throws Exception {
        String tableName = "append_only_table";
        createAppendOnlyTable(tableName);

        // Commit 1: snapshot 1
        writeAppendOnlyRows(tableName, 1, "Alice");
        // Commit 2: snapshot 2
        writeAppendOnlyRows(tableName, 2, "Bob");

        assertThat(capability.isAppendOnly(DATABASE + "." + tableName, 1L, 2L))
                .isTrue();
    }

    /**
     * Primary key table with changelog-producer=lookup: deleting a row
     * produces a COMPACT commit (Paimon merges changelog), which is
     * not append-only.
     */
    @Test
    void isAppendOnly_withUpdateOrDelete_returnsFalse() throws Exception {
        String tableName = "pk_table_with_delete";
        createPrimaryKeyTable(tableName);

        // Commit 1 (snapshot 1): insert rows
        writePrimaryKeyRows(tableName, 1, "Alice");
        long afterInsert = 1L;

        // Commit 2 (snapshot 2): delete a row — triggers non-APPEND commit
        deleteRow(tableName, 1, "Alice");
        long afterDelete = 2L;

        assertThat(capability.isAppendOnly(
                DATABASE + "." + tableName, afterInsert, afterDelete))
                .isFalse();
    }

    @Test
    void isAppendOnly_fromGreaterThanTo_throwsIllegalArgument() {
        assertThatThrownBy(() -> capability.isAppendOnly("db.any_table", 100L, 50L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must not be greater than");
    }

    @Test
    void isAppendOnly_sameExistingSnapshot_returnsTrue() throws Exception {
        String tableName = "same_existing_snapshot";
        createAppendOnlyTable(tableName);
        writeAppendOnlyRows(tableName, 1, "Alice");
        assertThat(capability.isAppendOnly(DATABASE + "." + tableName, 1L, 1L)).isTrue();
    }

    @Test
    void isAppendOnly_sameMissingSnapshotOrTable_isRejected() throws Exception {
        String tableName = "same_missing_snapshot";
        createAppendOnlyTable(tableName);
        writeAppendOnlyRows(tableName, 1, "Alice");
        assertThatThrownBy(() -> capability.isAppendOnly(DATABASE + "." + tableName, 42L, 42L))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Snapshot not found: 42");
        assertThatThrownBy(() -> capability.isAppendOnly(DATABASE + ".missing_table", 42L, 42L))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Table not found");
    }

    @Test
    void isAppendOnly_expiredBaseSnapshot_isRejected() throws Exception {
        String tableName = "expired_base_snapshot";
        createAppendOnlyTable(tableName);
        writeAppendOnlyRows(tableName, 1, "Alice");
        writeAppendOnlyRows(tableName, 2, "Bob");
        var table = (org.apache.paimon.table.FileStoreTable) catalog.getTable(Identifier.create(DATABASE, tableName));
        table.fileIO().delete(table.snapshotManager().snapshotPath(1), false);
        assertThat(table.snapshotManager().snapshotExists(1)).isFalse();
        assertThat(table.snapshotManager().snapshotExists(2)).isTrue();
        assertThatThrownBy(() -> capability.isAppendOnly(DATABASE + "." + tableName, 1L, 2L))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Snapshot not found: 1");
    }

    @Test
    void isAppendOnly_missingTargetSnapshot_isRejectedForPrimaryKeyTable() throws Exception {
        String tableName = "pk_missing_target_snapshot";
        createPrimaryKeyTable(tableName);
        writePrimaryKeyRows(tableName, 1, "Alice");
        assertThatThrownBy(() -> capability.isAppendOnly(DATABASE + "." + tableName, 1L, 42L))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Snapshot not found: 42");
    }

    @Test
    void isAppendOnly_nonPositiveEndpoints_areRejected() {
        assertThatThrownBy(() -> capability.isAppendOnly(DATABASE + ".any_table", 0L, 0L))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("must be positive");
        assertThatThrownBy(() -> capability.isAppendOnly(DATABASE + ".any_table", -1L, 1L))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("must be positive");
        assertThatThrownBy(() -> capability.isAppendOnly(DATABASE + ".any_table", 1L, -1L))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("must be positive");
    }

    // -- table creation helpers --

    private static void createAppendOnlyTable(String tableName) throws Exception {
        Schema schema = Schema.newBuilder()
                .column("id", DataTypes.INT())
                .column("name", DataTypes.STRING())
                .option("bucket", "1")
                .option("bucket-key", "id")
                .build();
        catalog.createTable(Identifier.create(DATABASE, tableName), schema, false);
    }

    private static void createPrimaryKeyTable(String tableName) throws Exception {
        Schema schema = Schema.newBuilder()
                .column("id", DataTypes.INT())
                .column("name", DataTypes.STRING())
                .primaryKey("id")
                .option("bucket", "1")
                .option("changelog-producer", "input")
                .build();
        catalog.createTable(Identifier.create(DATABASE, tableName), schema, false);
    }

    // -- write helpers --

    private static void writeAppendOnlyRows(String tableName, int id, String name)
            throws Exception {
        Table table = catalog.getTable(Identifier.create(DATABASE, tableName));
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = writeBuilder.newWrite();
             BatchTableCommit commit = writeBuilder.newCommit()) {
            write.write(GenericRow.of(id, BinaryString.fromString(name)));
            List<CommitMessage> messages = write.prepareCommit();
            commit.commit(messages);
        }
    }

    private static void writePrimaryKeyRows(String tableName, int id, String name)
            throws Exception {
        Table table = catalog.getTable(Identifier.create(DATABASE, tableName));
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = writeBuilder.newWrite();
             BatchTableCommit commit = writeBuilder.newCommit()) {
            write.write(GenericRow.of(id, BinaryString.fromString(name)));
            List<CommitMessage> messages = write.prepareCommit();
            commit.commit(messages);
        }
    }

    private static void deleteRow(String tableName, int id, String name)
            throws Exception {
        Table table = catalog.getTable(Identifier.create(DATABASE, tableName));
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = writeBuilder.newWrite();
             BatchTableCommit commit = writeBuilder.newCommit()) {
            GenericRow deleteRow = GenericRow.of(id, BinaryString.fromString(name));
            deleteRow.setRowKind(RowKind.DELETE);
            write.write(deleteRow);
            List<CommitMessage> messages = write.prepareCommit();
            commit.commit(messages);
        }
    }

    // -- catalog helper --

    private static Catalog buildCatalog() {
        Options options = new Options();
        options.setString("warehouse", warehouse.toString());
        CatalogContext context = CatalogContext.create(options);
        return CatalogFactory.createCatalog(context);
    }
}

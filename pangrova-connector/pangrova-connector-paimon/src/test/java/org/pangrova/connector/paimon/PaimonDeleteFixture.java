package org.pangrova.connector.paimon;

import org.pangrova.connector.api.session.ConnectorSession;
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
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Real filesystem catalog and immutable row assertions shared by metadata-delete tests. */
abstract class PaimonDeleteFixture {

    static final String DATABASE = "delete_db";
    static final String TABLE_NAME = "events";
    static final Identifier IDENTIFIER = Identifier.create(DATABASE, TABLE_NAME);
    static final ConnectorSession SESSION = new ConnectorSession() {
        @Override
        public java.util.Optional<org.pangrova.connector.api.query.ConnectorQueryAuthority> getQueryAuthority() {
            return java.util.Optional.empty();
        }

        @Override public String getUser() { return "delete_test"; }
        @Override public Optional<String> getCatalog() { return Optional.of("delete_catalog"); }
        @Override public Optional<String> getSchema() { return Optional.of(DATABASE); }
        @Override public Map<String, String> getProperties() { return Map.of("tenant_id", "test-tenant"); }
    };

    @TempDir
    Path warehouse;

    Catalog catalog;

    @BeforeEach
    void openCatalog() throws Exception {
        Options options = new Options();
        options.setString("warehouse", warehouse.toUri().toString());
        catalog = CatalogFactory.createCatalog(CatalogContext.create(options));
        catalog.createDatabase(DATABASE, false);
    }

    @AfterEach
    void closeCatalog() throws Exception {
        if (catalog != null) {
            catalog.close();
        }
    }

    Table createTable(boolean partitioned, boolean appendOnly) throws Exception {
        return createTable(partitioned, appendOnly, Map.of());
    }

    Table createTable(boolean partitioned, boolean appendOnly, Map<String, String> options)
            throws Exception {
        Schema.Builder schema = rowSchema().options(options)
                .option("bucket", appendOnly ? "-1" : "1");
        if (partitioned) {
            schema.partitionKeys("region", "day");
        }
        if (!appendOnly) {
            schema.primaryKey(partitioned ? List.of("id", "region", "day") : List.of("id"));
        }
        return createTable(schema.build());
    }

    Schema.Builder rowSchema() {
        return Schema.newBuilder()
                .column("id", DataTypes.BIGINT())
                .column("region", DataTypes.STRING())
                .column("day", DataTypes.STRING())
                .column("payload", DataTypes.STRING());
    }

    Table createTable(Schema schema) throws Exception {
        catalog.createTable(IDENTIFIER, schema, false);
        return freshTable();
    }

    Table freshTable() throws Exception {
        return catalog.getTable(IDENTIFIER);
    }

    void writeRows(StoredRow... rows) throws Exception {
        GenericRow[] values = new GenericRow[rows.length];
        for (int i = 0; i < rows.length; i++) {
            StoredRow row = rows[i];
            values[i] = GenericRow.of(row.id(), text(row.region()), text(row.day()), text(row.payload()));
        }
        writeRecords(values);
    }

    void writeRecords(GenericRow... rows) throws Exception {
        var builder = freshTable().newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite();
             BatchTableCommit commit = builder.newCommit()) {
            for (GenericRow row : rows) {
                write.write(row);
            }
            commit.commit(write.prepareCommit());
        }
    }

    List<StoredRow> readRows() throws Exception {
        var builder = freshTable().newReadBuilder().withProjection(new int[] {0, 1, 2, 3});
        List<StoredRow> rows = new ArrayList<>();
        // The iterator owns and closes the underlying reader; copy values before advancing it.
        try (var iterator = builder.newRead().createReader(builder.newScan().plan()).toCloseableIterator()) {
            while (iterator.hasNext()) {
                var row = iterator.next();
                rows.add(new StoredRow(row.getLong(0),
                        row.isNullAt(1) ? null : row.getString(1).toString(),
                        row.isNullAt(2) ? null : row.getString(2).toString(),
                        row.isNullAt(3) ? null : row.getString(3).toString()));
            }
        }
        return List.copyOf(rows);
    }

    long snapshotId() throws Exception {
        return freshTable().latestSnapshot().map(snapshot -> snapshot.id()).orElse(-1L);
    }

    static BinaryString text(String value) {
        return value == null ? null : BinaryString.fromString(value);
    }

    record StoredRow(long id, String region, String day, String payload) {}
}

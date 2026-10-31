package org.pangrova.connector.iceberg.write;

import org.pangrova.connector.iceberg.HadoopTestGuard;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import org.pangrova.connector.iceberg.IcebergMetadata;
import org.pangrova.connector.iceberg.IcebergSplitManager;
import org.pangrova.connector.iceberg.handle.IcebergSplit;
import org.pangrova.connector.iceberg.handle.IcebergTableHandle;
import org.pangrova.connector.iceberg.read.IcebergExpressionToSubstrait;
import org.pangrova.connector.spi.ConnectorSession;
import org.pangrova.connector.spi.ConnectorTransactionHandle;
import org.pangrova.connector.spi.data.ConnectorPageSink;
import org.pangrova.connector.spi.data.value.TypedRow;
import org.pangrova.connector.spi.data.page.Page;
import org.pangrova.connector.spi.data.type.PangrovaType;
import org.pangrova.connector.spi.metadata.ConnectorOutputTableHandle;
import org.pangrova.connector.spi.metadata.ConnectorSplit;
import org.pangrova.connector.spi.metadata.ConnectorSplitSource;
import org.pangrova.connector.spi.metadata.Constraint;
import org.pangrova.connector.spi.metadata.DynamicFilter;
import org.pangrova.connector.spi.metadata.table.ConnectorTableHandle;
import org.pangrova.connector.spi.metadata.table.ConnectorTableMetadata;
import org.pangrova.connector.spi.metadata.table.SchemaTableName;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Expressions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Regression for task 56 / F-CHAIN-WRITE-02: the merge-on-read RowDelta commit path (DELETE / MERGE)
 * must perform Iceberg conflict detection against the planned base snapshot, not only
 * {@code validateFromSnapshot}. Without {@code validateDataFilesExist(...).validateDeletedFiles()} a
 * DELETE planned from snapshot S0 whose referenced data file was concurrently removed (e.g. by a
 * concurrent OVERWRITE / compaction landing snapshot S1) commits a <b>dangling delete</b> pointing at
 * a data file that no longer exists — silently losing the update instead of forcing a retry.
 *
 * <p>Uses a real HadoopCatalog with Parquet files, mirroring the OVERWRITE coverage in
 * {@link ConflictValidatingCommitTest} but exercising the real RowDelta validation contract.
 */
class IcebergRowDeltaConflictTest {

    @TempDir
    static Path warehouse;

    private static Catalog catalog;
    private static IcebergMetadata metadata;
    private static IcebergSplitManager splitManager;
    private static IcebergPageSinkProvider pageSinkProvider;

    private static final String CATALOG_NAME = "rowdelta_conflict_catalog";
    private static final String NAMESPACE = "rowdelta_db";

    private static final ConnectorTransactionHandle TX = ConnectorTransactionHandle.empty();
    private static final DynamicFilter NO_FILTER = DynamicFilter.ALL;
    private static final Constraint NO_CONSTRAINT = Constraint.EMPTY;

    private static final ConnectorSession SESSION = new ConnectorSession() {
        @Override
        public java.util.Optional<org.pangrova.connector.spi.ConnectorQueryAuthority> getQueryAuthority() {
            return java.util.Optional.empty();
        }

        @Override public String getUser() { return "test"; }
        @Override public Optional<String> getCatalog() { return Optional.of(CATALOG_NAME); }
        @Override public Optional<String> getSchema() { return Optional.empty(); }
        @Override public Map<String, String> getProperties() { return Map.of("tenant_id", "test-tenant"); }
    };

    private static final List<ConnectorTableMetadata.Column> TEST_COLUMNS = List.of(
            new ConnectorTableMetadata.Column("id", PangrovaType.BIGINT, OptionalInt.of(1)),
            new ConnectorTableMetadata.Column("name", PangrovaType.VARCHAR, OptionalInt.of(2)));

    @BeforeAll
    static void setUp() {
        HadoopTestGuard.assumeHadoopCompatible();
        catalog = buildHadoopCatalog();
        if (catalog instanceof SupportsNamespaces nsCatalog) {
            nsCatalog.createNamespace(Namespace.of(NAMESPACE));
        }
        metadata = new IcebergMetadata(catalog, CATALOG_NAME);
        splitManager = new IcebergSplitManager(CATALOG_NAME, catalog, new IcebergExpressionToSubstrait());
        pageSinkProvider = new IcebergPageSinkProvider(catalog);
    }

    /**
     * Concurrency scenario (lost update / dangling delete):
     * <ol>
     *   <li>Writer A plans a DELETE from base snapshot S0; it produces a position-delete file that
     *       references data file F (rows 1,3).</li>
     *   <li>Writer B concurrently removes data file F (compaction / overwrite), landing snapshot S1.</li>
     *   <li>Writer A commits its RowDelta. With full Iceberg validation the commit MUST be rejected
     *       with {@link ValidationException} (F was deleted since S0), forcing a retry.</li>
     * </ol>
     */
    @Test
    void concurrentRemovalOfReferencedDataFileRejectsRowDelta() throws IOException {
        ConnectorOutputTableHandle outputHandle = createTestTable("dangling_delete_table");
        writeRows(outputHandle, buildRows(0, 5));

        Table table = loadIcebergTable("dangling_delete_table");
        long baseSnapshotId = table.currentSnapshot().snapshotId();
        String dataFilePath = findDataFilePath("dangling_delete_table");

        // Writer A: build a position-delete file referencing data file F, planned from S0.
        DeleteFile deleteFile = IcebergDeleteWriter.buildPositionDeleteFile(
                table, dataFilePath, List.of(1L, 3L));

        // Writer B: concurrently remove data file F, landing a new snapshot S1.
        table.newDelete().deleteFromRowFilter(Expressions.alwaysTrue()).commit();
        table.refresh();
        assertThat(table.currentSnapshot().snapshotId()).isNotEqualTo(baseSnapshotId);

        // Writer A commits its RowDelta against the now-stale base S0.
        Table forCommit = loadIcebergTable("dangling_delete_table");
        RowDelta rowDelta = forCommit.newRowDelta().addDeletes(deleteFile);
        List<CharSequence> referencedDataFiles = List.of(dataFilePath);

        // Full MOR validation: snapshot lineage + referenced-data-file existence. The concurrent
        // removal of data file F must be detected and the commit rejected (forcing a retry), rather
        // than committing a dangling delete. (Snapshot-only validation wrongly accepts this.)
        assertThatThrownBy(() ->
                ConflictValidatingCommit.validate(rowDelta, baseSnapshotId, referencedDataFiles).commit())
                .isInstanceOf(ValidationException.class);
    }

    // -- helpers --

    private static Catalog buildHadoopCatalog() {
        Map<String, String> props = Map.of(
                "type", "hadoop",
                "warehouse", warehouse.toString());
        return org.apache.iceberg.CatalogUtil.buildIcebergCatalog(
                CATALOG_NAME, props, new org.apache.hadoop.conf.Configuration());
    }

    private ConnectorOutputTableHandle createTestTable(String tableName) {
        SchemaTableName schemaTable = new SchemaTableName(NAMESPACE, tableName);
        ConnectorTableMetadata tableMeta = new ConnectorTableMetadata(
                schemaTable, List.of("id", "name"), TEST_COLUMNS, Map.of());
        return metadata.createTable(SESSION, tableMeta);
    }

    private void writeRows(ConnectorOutputTableHandle handle, List<Map<String, Object>> rows) {
        ConnectorPageSink sink = pageSinkProvider.createPageSink(TX, SESSION, handle);
        List<TypedRow> typedRows = rows.stream().map(TypedRow::fromRawMap).toList();
        sink.appendPage(new Page(List.of("id", "name"), typedRows));
        byte[] commitMetadata = sink.finishWithMetadata();
        metadata.finishDistributedWrite(SESSION, handle, List.of(commitMetadata));
        sink.close();
    }

    private List<Map<String, Object>> buildRows(int startInclusive, int endExclusive) {
        List<Map<String, Object>> rows = new ArrayList<>();
        for (int i = startInclusive; i < endExclusive; i++) {
            rows.add(Map.of("id", (long) i, "name", "row_" + i));
        }
        return rows;
    }

    private String findDataFilePath(String tableName) {
        List<IcebergSplit> splits = findSplits(tableName);
        assertThat(splits).isNotEmpty();
        return splits.get(0).filePath();
    }

    private List<IcebergSplit> findSplits(String tableName) {
        IcebergTableHandle handle = (IcebergTableHandle) loadTableHandle(tableName);
        ConnectorSplitSource source = splitManager.getSplits(
                TX, SESSION, handle, NO_FILTER, NO_CONSTRAINT);
        List<ConnectorSplit> splits = source.getNextBatch(100);
        source.close();
        return splits.stream().map(IcebergSplit.class::cast).toList();
    }

    private Table loadIcebergTable(String tableName) {
        IcebergTableHandle handle = (IcebergTableHandle) loadTableHandle(tableName);
        TableIdentifier id = TableIdentifier.of(handle.schemaName(), handle.tableName());
        return catalog.loadTable(id);
    }

    private ConnectorTableHandle loadTableHandle(String tableName) {
        return metadata.getTableHandle(SESSION, new SchemaTableName(NAMESPACE, tableName));
    }
}

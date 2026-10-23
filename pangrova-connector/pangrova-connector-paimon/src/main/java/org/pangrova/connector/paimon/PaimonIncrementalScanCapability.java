package org.pangrova.connector.paimon;

import org.pangrova.spi.connector.metadata.IncrementalScanCapability;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.utils.SnapshotManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Paimon implementation of {@link IncrementalScanCapability}.
 *
 * <p>Paimon supports both incremental append scan and changelog scan
 * natively. The {@link #isAppendOnly} method uses two strategies
 * depending on the table type:</p>
 * <ul>
 *   <li><b>Non-PK tables</b> (no primary key): inherently append-only.
 *       Paimon does not support deletes or updates on non-PK tables,
 *       so all commits are pure appends. Verified via snapshot
 *       {@link Snapshot.CommitKind}.</li>
 *   <li><b>PK tables</b> (with primary key): uses Paimon's changelog
 *       incremental scan ({@code incremental-between-scan-mode=CHANGELOG})
 *       to read the changelog between snapshots. If all changelog rows
 *       have {@link RowKind#INSERT}, the range is append-only. If the
 *       merge-on-read produces fewer rows than the split's declared
 *       row count, deletes or updates occurred.</li>
 * </ul>
 *
 * <p>Thread-safe: all fields are final and Paimon Catalog is safe for
 * concurrent reads.</p>
 */
public final class PaimonIncrementalScanCapability implements IncrementalScanCapability {

    private static final Logger LOG = LoggerFactory.getLogger(PaimonIncrementalScanCapability.class);

    private final Catalog catalog;

    public PaimonIncrementalScanCapability(Catalog catalog) {
        this.catalog = catalog;
    }

    @Override
    public boolean supportsIncrementalAppendScan() {
        return true;
    }

    @Override
    public boolean supportsChangelogScan() {
        return true;
    }

    /**
     * {@inheritDoc}
     *
     * <p>Paimon snapshot IDs are sequential (1, 2, 3, ...), so the
     * numeric comparison {@code fromSnapshotId > toSnapshotId} is a
     * direct validity check. When {@code fromSnapshotId == toSnapshotId},
     * the method returns {@code true} only after verifying that the snapshot exists.</p>
     */
    @Override
    public boolean isAppendOnly(String tableName, long fromSnapshotId, long toSnapshotId) {
        if (fromSnapshotId <= 0 || toSnapshotId <= 0) {
            throw new IllegalArgumentException("Snapshot identifiers must be positive");
        }
        if (fromSnapshotId > toSnapshotId) {
            throw new IllegalArgumentException(
                    "fromSnapshotId (" + fromSnapshotId
                            + ") must not be greater than toSnapshotId ("
                            + toSnapshotId + ")");
        }
        Table table = loadTable(tableName);
        SnapshotManager snapshots = ((FileStoreTable) table).snapshotManager();
        requireSnapshotExists(snapshots, fromSnapshotId);
        if (fromSnapshotId == toSnapshotId) {
            return true;
        }
        requireSnapshotExists(snapshots, toSnapshotId);

        if (table.primaryKeys().isEmpty()) {
            return isAppendOnlyByCommitKind(table, fromSnapshotId, toSnapshotId);
        }
        return isAppendOnlyByChangelogScan(table, fromSnapshotId, toSnapshotId, tableName);
    }

    private static void requireSnapshotExists(SnapshotManager snapshots, long snapshotId) {
        if (!snapshots.snapshotExists(snapshotId)) {
            throw new IllegalArgumentException("Snapshot not found: " + snapshotId);
        }
    }

    /**
     * Non-PK tables: Paimon does not support deletes or updates, so all
     * commits are pure appends. Verify via snapshot commitKind as a
     * safety check — only {@code APPEND} is considered append-only.
     */
    private boolean isAppendOnlyByCommitKind(
            Table table, long fromSnapshotId, long toSnapshotId) {
        FileStoreTable fileStoreTable = (FileStoreTable) table;
        SnapshotManager snapshotManager = fileStoreTable.snapshotManager();

        for (long id = fromSnapshotId + 1; id <= toSnapshotId; id++) {
            Snapshot snapshot = snapshotManager.snapshot(id);
            if (snapshot.commitKind() != Snapshot.CommitKind.APPEND) {
                LOG.debug("event=non_append_snapshot snapshot_id={} commit_kind={} table={}",
                        id, snapshot.commitKind(), table);
                return false;
            }
        }
        return true;
    }

    /**
     * PK tables: uses Paimon's changelog incremental scan to read the
     * changelog between snapshots. Checks each row's {@link RowKind}:
     * <ul>
     *   <li>If all rows are {@code INSERT} → append-only</li>
     *   <li>If any row is {@code DELETE}, {@code UPDATE_BEFORE}, or
     *       {@code UPDATE_AFTER} → not append-only</li>
     *   <li>If the merge-on-read produces fewer rows than the split's
     *       declared row count → deletes occurred (rows merged away)</li>
     * </ul>
     */
    private boolean isAppendOnlyByChangelogScan(
            Table table, long fromSnapshotId, long toSnapshotId, String tableName) {
        FileStoreTable fileStoreTable = (FileStoreTable) table;
        Table changelogView = fileStoreTable.copy(Map.of(
                "incremental-between-scan-mode", "CHANGELOG",
                "incremental-between", fromSnapshotId + "," + toSnapshotId
        ));

        ReadBuilder readBuilder = changelogView.newReadBuilder();
        List<Split> splits = readBuilder.newScan().plan().splits();
        if (splits.isEmpty()) {
            return true;
        }

        long declaredRowCount = countDeclaredRows(splits);
        long actualRowCount = scanChangelogRows(readBuilder, splits, tableName);
        return actualRowCount >= 0 && actualRowCount >= declaredRowCount;
    }

    private long scanChangelogRows(ReadBuilder readBuilder, List<Split> splits, String tableName) {
        try {
            TableRead tableRead = readBuilder.newRead();
            long actualRowCount = 0;
            for (Split split : splits) {
                long result = countAndCheckRows(tableRead, split);
                if (result < 0) {
                    LOG.debug("event=non_insert_row_detected table={}", tableName);
                    return -1;
                }
                actualRowCount += result;
            }
            return actualRowCount;
        } catch (IOException e) {
            throw new IllegalStateException(
                    "Failed to read changelog for table: " + tableName, e);
        }
    }

    /**
     * Reads rows from a split and checks their RowKind.
     *
     * @return the number of rows read, or -1 if a non-INSERT row is found
     */
    private long countAndCheckRows(TableRead tableRead, Split split) throws IOException {
        long count = 0;
        try (RecordReader<InternalRow> reader = tableRead.createReader(split)) {
            RecordReader.RecordIterator<InternalRow> batch;
            while ((batch = reader.readBatch()) != null) {
                long batchResult = countInsertOnlyBatch(batch);
                if (batchResult < 0) {
                    return -1;
                }
                count += batchResult;
            }
        }
        return count;
    }

    /**
     * Counts rows in a single batch, returning -1 if any non-INSERT row is found.
     */
    private long countInsertOnlyBatch(
            RecordReader.RecordIterator<InternalRow> batch) throws IOException {
        long count = 0;
        try {
            InternalRow row;
            while ((row = batch.next()) != null) {
                if (row.getRowKind() != RowKind.INSERT) {
                    return -1;
                }
                count++;
            }
        } finally {
            batch.releaseBatch();
        }
        return count;
    }

    private long countDeclaredRows(List<Split> splits) {
        long total = 0;
        for (Split split : splits) {
            if (split instanceof DataSplit dataSplit) {
                total += dataSplit.rowCount();
            }
        }
        return total;
    }

    private Table loadTable(String tableName) {
        Identifier id = parseIdentifier(tableName);
        try {
            Table table = catalog.getTable(id);
            if (!(table instanceof FileStoreTable)) {
                throw new IllegalArgumentException(
                        "Table is not a FileStoreTable: " + tableName);
            }
            return table;
        } catch (Catalog.TableNotExistException e) {
            throw new IllegalArgumentException("Table not found: " + tableName, e);
        }
    }

    /**
     * Parses a fully qualified table name into a Paimon {@link Identifier}.
     *
     * <p>Supports two formats:</p>
     * <ul>
     *   <li>{@code schema.table} — two-part name</li>
     *   <li>{@code catalog.schema.table} — three-part name
     *       (catalog prefix is stripped)</li>
     * </ul>
     */
    static Identifier parseIdentifier(String tableName) {
        String[] parts = tableName.split("\\.");
        return switch (parts.length) {
            case 2 -> Identifier.create(parts[0], parts[1]);
            case 3 -> Identifier.create(parts[1], parts[2]);
            default -> throw new IllegalArgumentException(
                    "Invalid table name format: '" + tableName
                            + "'. Expected 'schema.table' or 'catalog.schema.table'");
        };
    }
}

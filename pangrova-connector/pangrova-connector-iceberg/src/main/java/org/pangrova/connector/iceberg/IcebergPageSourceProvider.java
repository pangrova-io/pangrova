package org.pangrova.connector.iceberg;

import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.connector.iceberg.handle.IcebergColumnHandle;
import org.pangrova.connector.iceberg.handle.IcebergDeleteFile;
import org.pangrova.connector.iceberg.handle.IcebergSplit;
import org.pangrova.connector.iceberg.handle.IcebergTableHandle;
import org.pangrova.connector.iceberg.read.IcebergPageSource;
import org.pangrova.connector.iceberg.read.IcebergParquetReader;
import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.connector.api.transaction.ConnectorTransactionHandle;
import org.pangrova.connector.api.exception.ConnectorException;
import org.pangrova.connector.api.data.ConnectorPageSource;
import org.pangrova.connector.api.data.ConnectorPageSourceProvider;
import org.pangrova.connector.api.metadata.ConnectorSplit;
import org.pangrova.connector.api.metadata.DynamicFilter;
import org.pangrova.connector.api.metadata.table.ColumnHandle;
import org.pangrova.connector.api.metadata.table.ConnectorTableHandle;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Creates page sources for reading Iceberg data files via Parquet reader.
 * Delegates file I/O to IcebergParquetReader for anti-corruption isolation.
 *
 * <p>Requires a Catalog reference to resolve live Table objects from immutable handles.
 */
public class IcebergPageSourceProvider implements ConnectorPageSourceProvider {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergPageSourceProvider.class);

    /** Reserved Iceberg metadata column field ID for the position-delete {@code file_path} column. */
    private static final int DELETE_FILE_PATH_FIELD_ID = 2147483546;

    /** Reserved Iceberg metadata column field ID for the position-delete {@code pos} column. */
    private static final int DELETE_POS_FIELD_ID = 2147483545;

    private final Catalog catalog;

    public IcebergPageSourceProvider(Catalog catalog) {
        this.catalog = catalog;
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle table,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter) {
        IcebergSplit icebergSplit = (IcebergSplit) split;
        IcebergTableHandle icebergHandle = (IcebergTableHandle) table;
        List<IcebergColumnHandle> projected = toIcebergColumns(columns);

        // Resolve live Table from catalog using immutable handle metadata
        Table sdkTable = resolveTable(icebergHandle);

        LOG.debug("Opening page source for {} with {} columns, {} delete files",
                icebergSplit.filePath(), projected.size(), icebergSplit.deleteFiles().size());

        // Resolve deletes before opening the data file so unsupported delete
        // semantics fail closed without leaking a reader.
        java.util.Set<Long> deletedPositions = icebergSplit.deleteFiles().isEmpty()
                ? java.util.Set.of()
                : collectDeletedPositions(icebergSplit, sdkTable);

        CloseableIterable<Record> records = IcebergParquetReader.open(
                sdkTable, icebergSplit.filePath(), projected);

        if (!deletedPositions.isEmpty()) {
            records = CloseableIterable.filter(
                    records, new DeletePositionFilter(deletedPositions));
        }

        return new IcebergPageSource(records, projected);
    }

    /**
     * Collects all deleted row positions from position delete files and deletion vectors.
     *
     * @throws ConnectorException if the split carries an equality delete or a
     *         delete type this provider cannot interpret. Silently ignoring a
     *         delete would return rows that were deleted upstream.
     */
    private java.util.Set<Long> collectDeletedPositions(IcebergSplit split, Table sdkTable) {
        java.util.Set<Long> positions = new java.util.HashSet<>();
        for (IcebergDeleteFile deleteFile : split.deleteFiles()) {
            switch (deleteFile.deleteType()) {
                case "POSITION" -> readPositionDeletes(deleteFile, split, sdkTable, positions);
                case "DELETION_VECTOR" -> readDeletionVector(deleteFile, sdkTable, positions);
                case "EQUALITY" -> throw unsupportedDelete(
                        split, deleteFile, "equality deletes are not supported");
                default -> throw unsupportedDelete(
                        split, deleteFile, "unknown delete type");
            }
        }
        return positions;
    }

    private static ConnectorException unsupportedDelete(
            IcebergSplit split, IcebergDeleteFile deleteFile, String reason) {
        return new ConnectorException(
                PangrovaErrorCode.CONNECTOR_UNSUPPORTED_OPERATION,
                "Cannot read Iceberg table " + split.tableName()
                        + " at snapshot " + split.snapshotId() + ": " + reason
                        + " (delete file " + deleteFile.filePath()
                        + ", data file " + split.filePath() + ")");
    }

    private void readPositionDeletes(
            org.pangrova.connector.iceberg.handle.IcebergDeleteFile deleteFile,
            IcebergSplit split,
            Table sdkTable,
            java.util.Set<Long> positions) {
        try (CloseableIterable<Record> deletes = org.apache.iceberg.parquet.Parquet
                .read(sdkTable.io().newInputFile(deleteFile.filePath()))
                .project(positionDeleteSchema())
                .createReaderFunc(fileSchema ->
                        org.apache.iceberg.data.parquet.GenericParquetReaders.buildReader(
                                positionDeleteSchema(), fileSchema))
                .build()) {
            for (Record record : deletes) {
                String path = record.get(0, String.class);
                if (split.filePath().equals(path)) {
                    positions.add(record.get(1, Long.class));
                }
            }
        } catch (java.io.IOException e) {
            throw new org.pangrova.connector.api.exception.ConnectorException(
                    org.pangrova.common.enums.PangrovaErrorCode.CONNECTOR_READ_ERROR,
                    "Failed to read position deletes: " + deleteFile.filePath(), e);
        }
    }

    /**
     * Builds the read schema for position-delete files (file_path + pos metadata columns).
     */
    private static org.apache.iceberg.Schema positionDeleteSchema() {
        return new org.apache.iceberg.Schema(
                org.apache.iceberg.types.Types.NestedField.required(
                        DELETE_FILE_PATH_FIELD_ID, "file_path", org.apache.iceberg.types.Types.StringType.get()),
                org.apache.iceberg.types.Types.NestedField.required(
                        DELETE_POS_FIELD_ID, "pos", org.apache.iceberg.types.Types.LongType.get()));
    }

    private void readDeletionVector(
            org.pangrova.connector.iceberg.handle.IcebergDeleteFile deleteFile,
            Table sdkTable,
            java.util.Set<Long> positions) {
        String dvPath = deleteFile.dvFilePath().isEmpty()
                ? deleteFile.filePath() : deleteFile.dvFilePath();
        try {
            byte[] dvBytes = sdkTable.io().newInputFile(dvPath).newStream().readAllBytes();
            // Deletion vector is a serialized RoaringBitmap of deleted positions
            org.roaringbitmap.RoaringBitmap bitmap = new org.roaringbitmap.RoaringBitmap();
            bitmap.deserialize(java.nio.ByteBuffer.wrap(dvBytes));
            bitmap.forEach((org.roaringbitmap.IntConsumer) pos -> positions.add((long) pos));
        } catch (java.io.IOException e) {
            throw new org.pangrova.connector.api.exception.ConnectorException(
                    org.pangrova.common.enums.PangrovaErrorCode.CONNECTOR_READ_ERROR,
                    "Failed to read deletion vector: " + dvPath, e);
        }
    }

    /**
     * Predicate that filters out records at deleted positions.
     * Tracks the current row position as records are iterated.
     */
    private static final class DeletePositionFilter
            implements java.util.function.Predicate<Record> {
        private final java.util.Set<Long> deletedPositions;
        private long currentPosition;

        DeletePositionFilter(java.util.Set<Long> deletedPositions) {
            this.deletedPositions = deletedPositions;
            this.currentPosition = 0;
        }

        @Override
        public boolean test(Record record) {
            return !deletedPositions.contains(currentPosition++);
        }
    }

    /**
     * Resolves a live Iceberg Table from the catalog using handle metadata.
     */
    private Table resolveTable(IcebergTableHandle handle) {
        TableIdentifier id = TableIdentifier.of(handle.schemaName(), handle.tableName());
        return catalog.loadTable(id);
    }

    private List<IcebergColumnHandle> toIcebergColumns(List<ColumnHandle> columns) {
        return columns.stream()
                .map(IcebergColumnHandle.class::cast)
                .toList();
    }
}

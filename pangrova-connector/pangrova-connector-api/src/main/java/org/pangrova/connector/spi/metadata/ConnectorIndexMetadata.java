package org.pangrova.connector.spi.metadata;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.connector.spi.index.ColumnValueReader;
import org.pangrova.connector.spi.index.ConnectorIndexSupport;
import org.pangrova.connector.spi.index.FileSystemAccessor;
import org.pangrova.connector.api.index.NativeIndexCapability;
import org.pangrova.connector.spi.index.NativeIndexEvaluator;
import org.pangrova.connector.spi.index.SnapshotAwareMetadata;
import org.pangrova.connector.api.index.statistics.DataFileStatistics;

import java.util.List;
import java.util.Optional;

/**
 * Index-specific metadata operations for a connector.
 *
 * <p>Extracted from {@link ConnectorMetadata} per Interface Segregation Principle.
 * Connectors that do not support indexing need not implement these methods
 * beyond the default empty stubs.</p>
 */
@Experimental
public interface ConnectorIndexMetadata {

    /** Returns the file system accessor for reading/writing index files. */
    default Optional<FileSystemAccessor> getFileSystemAccessor() {
        return Optional.empty();
    }

    /** Returns the column value reader for extracting column data during index build. */
    default Optional<ColumnValueReader> getColumnValueReader() {
        return Optional.empty();
    }

    /** Returns the native index evaluator for L1 format-level evaluation. */
    default Optional<NativeIndexEvaluator> getNativeIndexEvaluator() {
        return Optional.empty();
    }

    /** Returns native index capabilities declared by this connector. */
    default List<NativeIndexCapability> nativeIndexCapabilities() {
        return List.of();
    }

    /** Lists current data file paths for the given table. */
    default List<String> listDataFiles(String catalogName, String schemaName, String tableName) {
        return List.of();
    }

    /** Returns the consolidated index support interface. */
    default Optional<ConnectorIndexSupport> getIndexSupport() {
        return Optional.empty();
    }

    /** Returns snapshot-aware metadata for incremental indexing. */
    default Optional<SnapshotAwareMetadata> getSnapshotAwareMetadata(
            String schemaName, String tableName) {
        return Optional.empty();
    }

    /** Returns column-level statistics for the specified data files. */
    default List<DataFileStatistics> getFileStatistics(
            String catalogName, String schemaName, String tableName,
            List<String> dataFilePaths) {
        return List.of();
    }
}

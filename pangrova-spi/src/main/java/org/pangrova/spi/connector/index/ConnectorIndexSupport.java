package org.pangrova.spi.connector.index;

import org.pangrova.spi.annotation.Experimental;

import java.util.List;
import java.util.Optional;

/**
 * Consolidates all index-related capabilities a connector may provide.
 *
 * <p>Connectors that support Pangrova's independent indexing layer implement
 * this interface and return it from
 * {@link org.pangrova.spi.connector.metadata.ConnectorMetadata#getIndexSupport}.</p>
 *
 * <p>All methods have safe defaults so connectors can implement only what
 * they support.</p>
 */
@Experimental
public interface ConnectorIndexSupport {

    /** Returns the file system accessor for reading data files and writing index files. */
    default Optional<FileSystemAccessor> getFileSystemAccessor() {
        return Optional.empty();
    }

    /** Returns the column value reader for extracting column values from data files. */
    default Optional<ColumnValueReader> getColumnValueReader() {
        return Optional.empty();
    }

    /** Registers a commit listener for write-aware incremental index builds. */
    default void registerCommitListener(CommitListener listener) {
        // no-op by default
    }

    /** Returns native index capabilities declared by this connector (L1 layer). */
    default List<NativeIndexCapability> nativeIndexCapabilities() {
        return List.of();
    }

    /** Returns the native index evaluator for L1 evaluation, if supported. */
    default Optional<NativeIndexEvaluator> getNativeIndexEvaluator() {
        return Optional.empty();
    }

    /**
     * Returns snapshot-aware metadata for the given table, enabling
     * snapshot-diff incremental builds.
     *
     * @param schemaName schema name
     * @param tableName  table name
     * @return snapshot-aware metadata, or empty if not supported
     */
    default Optional<SnapshotAwareMetadata> getSnapshotAwareMetadata(
            String schemaName, String tableName) {
        return Optional.empty();
    }

    /**
     * Lists current data files for the given table.
     *
     * @param catalogName catalog name
     * @param schemaName  schema name
     * @param tableName   table name
     * @return list of data file paths; empty if not supported
     */
    default List<String> listDataFiles(
            String catalogName, String schemaName, String tableName) {
        return List.of();
    }

    /**
     * Returns the snapshot pin provider for managing snapshot lifecycle
     * during index builds and garbage collection.
     *
     * @return snapshot pin provider, or empty if the connector does not support pinning
     */
    default Optional<SnapshotPinProvider> getSnapshotPinProvider() {
        return Optional.empty();
    }
}

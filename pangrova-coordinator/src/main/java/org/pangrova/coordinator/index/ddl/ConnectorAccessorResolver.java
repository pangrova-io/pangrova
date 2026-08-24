package org.pangrova.coordinator.index.ddl;

import org.pangrova.coordinator.index.IndexConnectorSession;
import org.pangrova.metadata.catalog.CatalogHandle;
import org.pangrova.metadata.catalog.CatalogRegistry;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.index.ColumnValueReader;
import org.pangrova.spi.connector.index.ConnectorIndexSupport;
import org.pangrova.spi.connector.index.FileSystemAccessor;
import org.pangrova.spi.connector.index.NativeIndexCapability;
import org.pangrova.spi.connector.index.NativeIndexEvaluator;
import org.pangrova.spi.connector.metadata.ConnectorMetadata;

import java.util.List;
import java.util.Optional;

/**
 * Resolves connector-specific accessors (FileSystemAccessor, ColumnValueReader,
 * NativeIndexEvaluator) from the catalog registry.
 *
 * <p>Prefers the consolidated {@link ConnectorIndexSupport} interface when
 * available, falling back to individual {@link ConnectorMetadata} methods
 * for backward compatibility with connectors that have not yet migrated.</p>
 *
 * <p>Instances are NOT cached — connector storage configuration may change
 * at runtime, so each resolution fetches fresh from the connector.</p>
 */
public final class ConnectorAccessorResolver {

    private final CatalogRegistry catalogRegistry;

    public ConnectorAccessorResolver(CatalogRegistry catalogRegistry) {
        this.catalogRegistry = catalogRegistry;
    }

    /**
     * Resolves the file system accessor for the given catalog.
     *
     * @param catalogName catalog to resolve
     * @return the file system accessor
     * @throws IllegalArgumentException if catalog not registered or accessor not provided
     */
    public FileSystemAccessor resolve(String catalogName) {
        ConnectorMetadata metadata = getMetadata(catalogName);
        Optional<ConnectorIndexSupport> support = metadata.getIndexSupport();
        if (support.isPresent()) {
            return support.get().getFileSystemAccessor()
                    .orElseThrow(() -> new IllegalArgumentException(
                            "Catalog '" + catalogName + "' does not provide a FileSystemAccessor"));
        }
        return metadata.getFileSystemAccessor()
                .orElseThrow(() -> new IllegalArgumentException(
                        "Catalog '" + catalogName + "' does not provide a FileSystemAccessor"));
    }

    /**
     * Resolves the column value reader for the given catalog.
     *
     * @param catalogName catalog to resolve
     * @return the column value reader
     * @throws IllegalArgumentException if catalog not registered or reader not provided
     */
    public ColumnValueReader resolveColumnValueReader(String catalogName) {
        ConnectorMetadata metadata = getMetadata(catalogName);
        Optional<ConnectorIndexSupport> support = metadata.getIndexSupport();
        if (support.isPresent()) {
            return support.get().getColumnValueReader()
                    .orElseThrow(() -> new IllegalArgumentException(
                            "Catalog '" + catalogName + "' does not provide a ColumnValueReader"));
        }
        return metadata.getColumnValueReader()
                .orElseThrow(() -> new IllegalArgumentException(
                        "Catalog '" + catalogName + "' does not provide a ColumnValueReader"));
    }

    /**
     * Resolves the native index evaluator for the given catalog, or null if not supported.
     *
     * @param catalogName catalog to resolve
     * @return the native index evaluator, or null
     */
    public NativeIndexEvaluator resolveNativeIndexEvaluator(String catalogName) {
        ConnectorMetadata metadata = getMetadata(catalogName);
        Optional<ConnectorIndexSupport> support = metadata.getIndexSupport();
        if (support.isPresent()) {
            return support.get().getNativeIndexEvaluator().orElse(null);
        }
        return metadata.getNativeIndexEvaluator().orElse(null);
    }

    /**
     * Resolves native index capabilities declared by the given catalog.
     *
     * @param catalogName catalog to resolve
     * @return list of native capabilities (empty if none)
     */
    public List<NativeIndexCapability> resolveNativeCapabilities(String catalogName) {
        ConnectorMetadata metadata = getMetadata(catalogName);
        Optional<ConnectorIndexSupport> support = metadata.getIndexSupport();
        if (support.isPresent()) {
            return support.get().nativeIndexCapabilities();
        }
        return metadata.nativeIndexCapabilities();
    }

    /**
     * Lists current data files for the given table via the connector.
     *
     * @param catalogName catalog name
     * @param schemaName  schema name
     * @param tableName   table name
     * @return list of data file paths; empty if connector does not support listing
     */
    public List<String> listDataFiles(String catalogName, String schemaName, String tableName) {
        ConnectorMetadata metadata = getMetadata(catalogName);
        Optional<ConnectorIndexSupport> support = metadata.getIndexSupport();
        if (support.isPresent()) {
            return support.get().listDataFiles(catalogName, schemaName, tableName);
        }
        return metadata.listDataFiles(catalogName, schemaName, tableName);
    }

    /**
     * Registers a commit listener with the connector for the given catalog.
     *
     * @param catalogName catalog name
     * @param listener    the commit listener to register
     */
    public void registerCommitListener(
            String catalogName,
            org.pangrova.spi.connector.index.CommitListener listener) {
        ConnectorMetadata metadata = getMetadata(catalogName);
        Optional<ConnectorIndexSupport> support = metadata.getIndexSupport();
        if (support.isPresent()) {
            support.get().registerCommitListener(listener);
            return;
        }
        metadata.registerCommitListener(listener);
    }

    /**
     * Resolves snapshot-aware metadata for the given catalog and table, or null if not supported.
     *
     * @param catalogName catalog to resolve
     * @param schemaName  schema name
     * @param tableName   table name
     * @return the snapshot-aware metadata, or null
     */
    public org.pangrova.spi.connector.index.SnapshotAwareMetadata resolveSnapshotAwareMetadata(
            String catalogName, String schemaName, String tableName) {
        ConnectorMetadata metadata = getMetadata(catalogName);
        Optional<ConnectorIndexSupport> support = metadata.getIndexSupport();
        if (support.isPresent()) {
            return support.get().getSnapshotAwareMetadata(schemaName, tableName).orElse(null);
        }
        return metadata.getSnapshotAwareMetadata(schemaName, tableName).orElse(null);
    }

    /**
     * Resolves file-level column statistics from the connector for the given data files.
     *
     * @param catalogName catalog name
     * @param schemaName  schema name
     * @param tableName   table name
     * @param dataFilePaths data file paths to collect statistics for
     * @return per-file statistics; empty list if connector does not support statistics
     */
    public List<org.pangrova.spi.index.statistics.DataFileStatistics> resolveFileStatistics(
            String catalogName, String schemaName, String tableName,
            List<String> dataFilePaths) {
        ConnectorMetadata metadata = getMetadata(catalogName);
        return metadata.getFileStatistics(catalogName, schemaName, tableName, dataFilePaths);
    }

    /**
     * Resolves the snapshot pin provider from the first catalog that supports it.
     * Returns null if no catalog provides snapshot pinning.
     *
     * @return the snapshot pin provider, or null
     */
    public org.pangrova.spi.connector.index.SnapshotPinProvider resolveSnapshotPinProvider() {
        for (String catalogName : catalogRegistry.listCatalogNames()) {
            ConnectorMetadata metadata = getMetadata(catalogName);
            Optional<org.pangrova.spi.connector.index.ConnectorIndexSupport> support =
                    metadata.getIndexSupport();
            if (support.isPresent()) {
                Optional<org.pangrova.spi.connector.index.SnapshotPinProvider> pin =
                        support.get().getSnapshotPinProvider();
                if (pin.isPresent()) {
                    return pin.get();
                }
            }
        }
        return null;
    }

    /**
     * Resolves real DataFileInfo with row counts and sizes from connector metadata.
     * Replaces the pattern of creating DataFileInfo with zeros for unknown row counts.
     *
     * @param catalogName   catalog name
     * @param schemaName    schema name
     * @param tableName     table name
     * @param dataFilePaths data file paths to resolve
     * @return list of DataFileInfo with real row counts where available
     */
    public java.util.List<org.pangrova.indexing.build.DataFileInfo> resolveDataFileInfos(
            String catalogName, String schemaName, String tableName,
            List<String> dataFilePaths) {
        java.util.List<org.pangrova.spi.index.statistics.DataFileStatistics> stats =
                resolveFileStatistics(catalogName, schemaName, tableName, dataFilePaths);
        java.util.Map<String, org.pangrova.spi.index.statistics.DataFileStatistics> statsMap =
                new java.util.HashMap<>();
        for (org.pangrova.spi.index.statistics.DataFileStatistics s : stats) {
            statsMap.put(s.dataFilePath(), s);
        }
        java.util.List<org.pangrova.indexing.build.DataFileInfo> result = new java.util.ArrayList<>();
        for (String path : dataFilePaths) {
            org.pangrova.spi.index.statistics.DataFileStatistics s = statsMap.get(path);
            if (s != null) {
                result.add(new org.pangrova.indexing.build.DataFileInfo(
                        path, s.rowCount(), s.fileSizeBytes(), detectFormat(path)));
            } else {
                result.add(new org.pangrova.indexing.build.DataFileInfo(path, 0, 0, detectFormat(path)));
            }
        }
        return result;
    }

    /**
     * Combines {@link #listDataFiles} and {@link #resolveDataFileInfos}.
     * Lists all current data files for a table, then resolves their metadata.
     */
    public java.util.List<org.pangrova.indexing.build.DataFileInfo> listDataFilesDetailed(
            String catalogName, String schemaName, String tableName) {
        List<String> files = listDataFiles(catalogName, schemaName, tableName);
        if (files.isEmpty()) {
            return java.util.List.of();
        }
        return resolveDataFileInfos(catalogName, schemaName, tableName, files);
    }

    /** Detects file format from path extension. */
    private static String detectFormat(String path) {
        if (path == null) return "unknown";
        String lower = path.toLowerCase();
        if (lower.endsWith(".parquet")) return "parquet";
        if (lower.endsWith(".orc")) return "orc";
        if (lower.endsWith(".avro")) return "avro";
        return "unknown";
    }

    private ConnectorMetadata getMetadata(String catalogName) {
        CatalogHandle handle = catalogRegistry.getCatalog(catalogName);
        if (handle == null) {
            throw new IllegalArgumentException(
                    "Catalog '" + catalogName + "' is not registered");
        }
        ConnectorSession session = new IndexConnectorSession(catalogName);
        return handle.getMetadata(session);
    }
}

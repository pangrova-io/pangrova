package org.pangrova.coordinator.indexing.connector;

import org.pangrova.metadata.api.catalog.CatalogHandle;
import org.pangrova.indexing.api.evaluation.CatalogIndexEvaluation;
import org.pangrova.metadata.catalog.registry.CatalogRegistry;
import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.connector.api.index.ColumnValueReader;
import org.pangrova.connector.api.index.ConnectorIndexSupport;
import org.pangrova.connector.api.index.FileSystemAccessor;
import org.pangrova.connector.api.index.NativeIndexCapability;
import org.pangrova.connector.api.index.NativeIndexEvaluator;
import org.pangrova.connector.api.metadata.ConnectorMetadata;

import java.util.List;
import java.util.Map;
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
public final class ConnectorAccessorResolver implements CatalogIndexEvaluation {

    private final CatalogRegistry catalogRegistry;
    private final ConnectorSession session;

    public ConnectorAccessorResolver(CatalogRegistry catalogRegistry) {
        this(catalogRegistry, null);
    }

    private ConnectorAccessorResolver(CatalogRegistry catalogRegistry, ConnectorSession session) {
        this.catalogRegistry = catalogRegistry;
        this.session = session;
    }

    /**
     * Binds this process resource to an immutable, exactly resolved connector operation.
     *
     * @param authenticatedSession trusted connector session supplied by the query authority
     * @return a resolver scoped to the supplied tenant, principal and catalog/schema
     * @throws org.pangrova.security.api.exception.AccessDeniedException if the scope is incomplete
     */
    public ConnectorAccessorResolver forSession(ConnectorSession authenticatedSession) {
        if (authenticatedSession == null || authenticatedSession.getUser() == null
                || authenticatedSession.getUser().isBlank()
                || authenticatedSession.getProperties().getOrDefault("tenant_id", "").isBlank()
                || authenticatedSession.getCatalog().isEmpty() || authenticatedSession.getSchema().isEmpty()) {
            throw new org.pangrova.security.api.exception.AccessDeniedException(
                    "Index connector access requires an authenticated tenant and resolved catalog/schema");
        }
        return new ConnectorAccessorResolver(catalogRegistry, new ScopedSession(
                authenticatedSession.getUser(), authenticatedSession.getCatalog(), authenticatedSession.getSchema(),
                Map.copyOf(authenticatedSession.getProperties()), authenticatedSession.getQueryAuthority()));
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
    @Override
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
    @Override
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
        ConnectorMetadata metadata = getMetadata(catalogName, schemaName);
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
            org.pangrova.connector.api.index.CommitListener listener) {
        catalogRegistry.registerCommitListener(catalogName, listener);
    }

    /**
     * Resolves snapshot-aware metadata for the given catalog and table, or null if not supported.
     *
     * @param catalogName catalog to resolve
     * @param schemaName  schema name
     * @param tableName   table name
     * @return the snapshot-aware metadata, or null
     */
    public org.pangrova.connector.api.index.SnapshotAwareMetadata resolveSnapshotAwareMetadata(
            String catalogName, String schemaName, String tableName) {
        ConnectorMetadata metadata = getMetadata(catalogName, schemaName);
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
    public List<org.pangrova.connector.api.index.statistics.DataFileStatistics> resolveFileStatistics(
            String catalogName, String schemaName, String tableName,
            List<String> dataFilePaths) {
        ConnectorMetadata metadata = getMetadata(catalogName, schemaName);
        return metadata.getFileStatistics(catalogName, schemaName, tableName, dataFilePaths);
    }

    /**
     * Resolves snapshot pinning only within the authorized catalog.
     * Returns null if the authorized catalog does not provide snapshot pinning.
     *
     * @return the snapshot pin provider, or null
     */
    public org.pangrova.connector.api.index.SnapshotPinProvider resolveSnapshotPinProvider() {
        requireScope();
        return getMetadata(session.getCatalog().orElseThrow()).getIndexSupport()
                .flatMap(ConnectorIndexSupport::getSnapshotPinProvider).orElse(null);
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
        java.util.List<org.pangrova.connector.api.index.statistics.DataFileStatistics> stats =
                resolveFileStatistics(catalogName, schemaName, tableName, dataFilePaths);
        java.util.Map<String, org.pangrova.connector.api.index.statistics.DataFileStatistics> statsMap =
                new java.util.HashMap<>();
        for (org.pangrova.connector.api.index.statistics.DataFileStatistics s : stats) {
            statsMap.put(s.dataFilePath(), s);
        }
        java.util.List<org.pangrova.indexing.build.DataFileInfo> result = new java.util.ArrayList<>();
        for (String path : dataFilePaths) {
            org.pangrova.connector.api.index.statistics.DataFileStatistics s = statsMap.get(path);
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
        requireScope();
        if (!session.getCatalog().orElseThrow().equals(catalogName)) {
            throw new org.pangrova.security.api.exception.AccessDeniedException(
                    "Index connector scope does not match the requested catalog");
        }
        CatalogHandle handle = catalogRegistry.getCatalog(catalogName);
        if (handle == null) {
            throw new IllegalArgumentException(
                    "Catalog '" + catalogName + "' is not registered");
        }
        return handle.getMetadata(session);
    }

    private ConnectorMetadata getMetadata(String catalogName, String schemaName) {
        requireScope();
        if (!session.getSchema().orElseThrow().equals(schemaName)) {
            throw new org.pangrova.security.api.exception.AccessDeniedException(
                    "Index connector scope does not match the requested schema");
        }
        return getMetadata(catalogName);
    }

    private void requireScope() {
        if (session == null) {
            throw new org.pangrova.security.api.exception.AccessDeniedException(
                    "Index connector access requires a trusted authorization scope; "
                            + "background maintenance must reauthorize before metadata lookup");
        }
    }

    private record ScopedSession(String user, Optional<String> catalog, Optional<String> schema,
                                 Map<String, String> properties,
                                 Optional<org.pangrova.connector.api.query.ConnectorQueryAuthority> queryAuthority)
            implements ConnectorSession {
        @Override public String getUser() { return user; }
        @Override public Optional<String> getCatalog() { return catalog; }
        @Override public Optional<String> getSchema() { return schema; }
        @Override public Map<String, String> getProperties() { return properties; }
        @Override public Optional<org.pangrova.connector.api.query.ConnectorQueryAuthority> getQueryAuthority() {
            return queryAuthority;
        }
    }
}

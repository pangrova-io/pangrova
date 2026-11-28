package org.pangrova.connector.paimon;

import org.pangrova.connector.api.index.ColumnValueReader;
import org.pangrova.connector.api.index.ConnectorIndexCapabilities;
import org.pangrova.connector.api.index.FileSystemAccessor;
import org.pangrova.connector.api.index.NativeIndexCapability;
import org.pangrova.connector.api.index.NativeIndexEvaluator;
import org.pangrova.connector.api.index.SnapshotAwareMetadata;
import org.pangrova.connector.api.index.SnapshotPinProvider;
import org.apache.paimon.catalog.Catalog;

import java.util.List;
import java.util.Optional;

/**
 * Bundles all index-related capabilities for the Paimon connector.
 * Delegates to the enclosing {@link PaimonMetadata}.
 */
final class PaimonIndexCapabilities implements ConnectorIndexCapabilities {

    private final PaimonMetadata metadata;
    private final Catalog catalog;

    PaimonIndexCapabilities(PaimonMetadata metadata, Catalog catalog) {
        this.metadata = metadata;
        this.catalog = catalog;
    }

    @Override
    public Optional<FileSystemAccessor> getFileSystemAccessor() {
        return metadata.getFileSystemAccessor();
    }

    @Override
    public Optional<ColumnValueReader> getColumnValueReader() {
        return metadata.getColumnValueReader();
    }

    @Override
    public List<NativeIndexCapability> nativeIndexCapabilities() {
        return metadata.nativeIndexCapabilities();
    }

    @Override
    public Optional<NativeIndexEvaluator> getNativeIndexEvaluator() {
        return metadata.getNativeIndexEvaluator();
    }

    @Override
    public Optional<SnapshotAwareMetadata> getSnapshotAwareMetadata(
            String schemaName, String tableName) {
        return metadata.getSnapshotAwareMetadata(schemaName, tableName);
    }

    @Override
    public Optional<SnapshotPinProvider> getSnapshotPinProvider() {
        return Optional.of(new PaimonSnapshotPinProvider(catalog));
    }

    @Override
    public List<String> listDataFiles(String targetCatalogName, String schemaName, String tableName) {
        return metadata.listDataFiles(targetCatalogName, schemaName, tableName);
    }
}

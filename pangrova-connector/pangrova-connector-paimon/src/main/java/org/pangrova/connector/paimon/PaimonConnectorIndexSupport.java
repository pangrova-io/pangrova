package org.pangrova.connector.paimon;

import org.pangrova.spi.connector.index.ColumnValueReader;
import org.pangrova.spi.connector.index.ConnectorIndexSupport;
import org.pangrova.spi.connector.index.FileSystemAccessor;
import org.pangrova.spi.connector.index.NativeIndexCapability;
import org.pangrova.spi.connector.index.NativeIndexEvaluator;
import org.pangrova.spi.connector.index.SnapshotAwareMetadata;

import java.util.List;
import java.util.Optional;

/**
 * Bundles all index-related capabilities for the Paimon connector.
 * Delegates to the enclosing {@link PaimonMetadata}.
 */
final class PaimonConnectorIndexSupport implements ConnectorIndexSupport {

    private final PaimonMetadata metadata;

    PaimonConnectorIndexSupport(PaimonMetadata metadata) {
        this.metadata = metadata;
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
    public List<String> listDataFiles(String targetCatalogName, String schemaName, String tableName) {
        return metadata.listDataFiles(targetCatalogName, schemaName, tableName);
    }
}

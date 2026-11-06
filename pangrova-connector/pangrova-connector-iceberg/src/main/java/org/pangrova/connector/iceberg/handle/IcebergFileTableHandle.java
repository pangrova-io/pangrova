package org.pangrova.connector.iceberg.handle;

import java.util.List;
import java.util.Objects;

import org.pangrova.connector.api.metadata.table.ConnectorTableHandle;
import org.pangrova.connector.api.metadata.table.ConnectorTableMetadata;

/** A read-only Iceberg $files selection pinned to one manifest object graph. */
public record IcebergFileTableHandle(
        IcebergManifestTableHandle manifestSelection,
        List<ManifestObject> manifests,
        List<ConnectorTableMetadata.Column> typedColumns,
        boolean entries) implements ConnectorTableHandle {

    public IcebergFileTableHandle(IcebergManifestTableHandle manifestSelection,
            List<ManifestObject> manifests, List<ConnectorTableMetadata.Column> typedColumns) {
        this(manifestSelection, manifests, typedColumns, false);
    }

    private static final String SHA256_HEX_PATTERN = "[0-9a-f]{64}";

    /** One child manifest verified independently before the Native scan. */
    public record ManifestObject(String uri, long length, String sha256, int specId, int content) {
        public ManifestObject {
            if (uri == null || uri.isBlank() || length <= 0 || sha256 == null
                    || !sha256.matches(SHA256_HEX_PATTERN) || specId < 0
                    || (content != 0 && content != 1)) {
                throw new IllegalArgumentException("Invalid pinned Iceberg child manifest");
            }
        }
    }

    public IcebergFileTableHandle {
        Objects.requireNonNull(manifestSelection, "manifestSelection");
        manifests = List.copyOf(Objects.requireNonNull(manifests, "manifests"));
        typedColumns = List.copyOf(Objects.requireNonNull(typedColumns, "typedColumns"));
        if (typedColumns.isEmpty() || (manifestSelection.empty() && !manifests.isEmpty())) {
            throw new IllegalArgumentException("Invalid Iceberg files selection");
        }
    }

    public IcebergTableHandle baseTable() {
        return manifestSelection.baseTable();
    }

    public String tableName() {
        return baseTable().tableName() + (entries ? "$entries" : "$files");
    }
}

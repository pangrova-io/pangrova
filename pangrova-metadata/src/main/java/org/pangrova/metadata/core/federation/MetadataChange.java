package org.pangrova.metadata.core.federation;

import org.pangrova.metadata.api.schema.TableMetadata;
import org.pangrova.metadata.api.object.QualifiedTableRef;

/**
 * Represents a single metadata change detected during synchronization.
 */
public record MetadataChange(ChangeType type, QualifiedTableRef tableRef, TableMetadata metadata) {

    public enum ChangeType {
        ADD,
        UPDATE,
        DELETE
    }
}

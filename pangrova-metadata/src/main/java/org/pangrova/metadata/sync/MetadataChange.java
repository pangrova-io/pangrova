package org.pangrova.metadata.sync;

import org.pangrova.metadata.model.TableMetadata;
import org.pangrova.metadata.model.QualifiedTableRef;

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

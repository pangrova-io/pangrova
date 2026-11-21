package org.pangrova.metadata.api.catalog;

import org.pangrova.metadata.api.enums.MetadataChangeType;
import org.pangrova.metadata.api.object.TableMetadata;
import org.pangrova.metadata.api.object.QualifiedTableRef;

/**
 * Represents a single metadata change detected during synchronization.
 */
public record MetadataChange(MetadataChangeType type, QualifiedTableRef tableRef, TableMetadata metadata) {

}

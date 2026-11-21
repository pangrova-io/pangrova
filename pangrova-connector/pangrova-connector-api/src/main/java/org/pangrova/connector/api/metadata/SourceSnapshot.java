package org.pangrova.connector.api.metadata;

import org.pangrova.common.api.annotation.Experimental;

/** One accepted historical snapshot and its format-owned metadata. */
@Experimental
public record SourceSnapshot(SourceTableState table, AcceptedSourceSnapshot reference) {
    public SourceSnapshot {
        java.util.Objects.requireNonNull(table);
        java.util.Objects.requireNonNull(reference);
    }
}

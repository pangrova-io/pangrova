package org.pangrova.persistence.catalog.api.projection;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.metadata.api.object.TableMetadata;
import org.pangrova.metadata.api.object.QualifiedTableRef;
import java.util.List;
import java.util.Optional;

/**
 * Local, rebuildable table projections, not external schema/snapshot authority.
 * Read-only and thread-safe. Mutations belong exclusively to {@link MetadataProjectionTransactions}.
 * Read failure must not become absence.
 * Callers borrow this port; shared backend resources remain with assembly.
 * These operations do not promise a source-version compare or a complete inventory snapshot.
 */
@Experimental
public interface TableProjectionRepository {
    Optional<TableMetadata> loadTable(String catalog, String schema, String table);
    /** Literal substring search, capped at 100 matches. Result ordering is unspecified. */
    List<QualifiedTableRef> searchTables(String pattern);
}

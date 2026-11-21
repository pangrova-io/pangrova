package org.pangrova.persistence.catalog.api.projection;

import org.pangrova.common.api.annotation.Experimental;
import java.util.List;

/**
 * Thread-safe reader of locally observed schemas, not a complete or authoritative external inventory.
 * Mutations belong exclusively to {@link MetadataProjectionTransactions}. Read errors must not
 * become empty results. Returned names are an immutable, lexicographically ordered snapshot.
 * Borrowed backend resources remain with assembly.
 */
@Experimental
public interface SchemaProjectionRepository {
    List<String> loadSchemas(String catalogName);
}

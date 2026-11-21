package org.pangrova.metadata.api.catalog;

import org.pangrova.common.api.annotation.Experimental;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Incremental changes and optional source-certified inclusive coverage. For a read after C,
 * completeThrough=W guarantees all source changes in (C,W] are represented and no later commit
 * can introduce a change in that interval. Pagination, an empty query or a local clock reading
 * alone do not establish this guarantee. A source without such a cursor returns an empty bound;
 * its changes can be applied, but consumers must replay from C next time. Deletions are changes too.
 */
@Experimental
public record MetadataChangeBatch(List<MetadataChange> changes, Optional<Instant> completeThrough) {
    public MetadataChangeBatch {
        changes = List.copyOf(changes);
        Objects.requireNonNull(completeThrough, "completeThrough");
    }
}

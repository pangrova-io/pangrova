package org.pangrova.persistence.catalog.api.projection;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.metadata.api.object.TableMetadata;

import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Durable result of one atomic mutation. Previous projection and tenant impact were read under
 * the same deployment lock as the write, not guessed from an earlier compiler observation.
 * Historical replay confirms that operation only; it does not grant current deployment authority.
 * The changed flag describes the original operation's affected rows, including observation
 * timestamps. It is not a claim that a replay wrote again or that source columns changed.
 */
@Experimental
public record MetadataProjectionReceipt(MetadataProjectionMutation mutation, boolean changed,
        Optional<TableMetadata> previousTable, Set<String> allowedTenants) {
    public MetadataProjectionReceipt {
        Objects.requireNonNull(mutation, "mutation");
        Objects.requireNonNull(previousTable, "previousTable");
        allowedTenants = Set.copyOf(allowedTenants);
        if (mutation instanceof MetadataProjectionMutation.ObserveSchemas && previousTable.isPresent()) {
            throw new IllegalArgumentException("A schema observation has no previous table");
        }
    }
}

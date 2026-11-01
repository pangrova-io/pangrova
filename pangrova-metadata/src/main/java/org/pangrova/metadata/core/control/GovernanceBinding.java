package org.pangrova.metadata.core.control;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * One governance artifact bound to a stable {@code table_id}.
 *
 * <p>The reference ({@code bindingRef}) identifies the artifact inside its
 * governance store (for example a column tag key, mask id, or rule id). It is
 * deliberately scoped by {@code table_id}: the same reference on a table that
 * was dropped and recreated is a different binding and must be re-created.</p>
 */
public record GovernanceBinding(
        UUID bindingId,
        UUID tableId,
        GovernanceBindingType bindingType,
        String bindingRef,
        Map<String, String> config,
        String createdBy,
        Instant createdAt,
        Instant updatedAt) {

    public GovernanceBinding {
        Objects.requireNonNull(bindingId, "bindingId");
        Objects.requireNonNull(tableId, "tableId");
        Objects.requireNonNull(bindingType, "bindingType");
        if (bindingRef == null || bindingRef.isBlank()) {
            throw new IllegalArgumentException("bindingRef must not be blank");
        }
        config = config == null ? Map.of() : Map.copyOf(config);
        createdBy = createdBy == null ? "" : createdBy;
        Objects.requireNonNull(createdAt, "createdAt");
        Objects.requireNonNull(updatedAt, "updatedAt");
    }

    public GovernanceBinding withConfig(Map<String, String> newConfig, Instant now) {
        return new GovernanceBinding(bindingId, tableId, bindingType, bindingRef,
                newConfig, createdBy, createdAt, now);
    }
}

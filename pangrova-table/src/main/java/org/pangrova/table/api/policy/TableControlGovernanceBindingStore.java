package org.pangrova.table.api.policy;

import org.pangrova.table.api.enums.GovernanceBindingType;

import org.pangrova.common.api.annotation.Internal;


import java.util.List;
import java.util.Optional;
import java.util.UUID;

/** Durable authority for governance bindings keyed by {@code table_id}. */
@Internal
public interface TableControlGovernanceBindingStore {

    /**
     * Atomically inserts or updates the exact (tableId, bindingType, bindingRef) key.
     * An update replaces config and updatedAt, preserving the winning insert's
     * bindingId, createdBy and createdAt even when initial creators race.
     * Returns the immutable row established by this operation, not the unconfirmed
     * input or a later read that another writer could have changed. A failed response
     * does not prove rollback when the commit outcome is unknown; confirm through find.
     */
    GovernanceBinding save(GovernanceBinding binding);

    Optional<GovernanceBinding> find(UUID tableId, GovernanceBindingType type,
            String bindingRef);

    List<GovernanceBinding> list(UUID tableId);

    void delete(UUID tableId, GovernanceBindingType type, String bindingRef);

    void deleteAll(UUID tableId);
}

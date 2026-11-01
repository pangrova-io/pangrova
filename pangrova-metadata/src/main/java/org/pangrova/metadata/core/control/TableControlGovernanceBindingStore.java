package org.pangrova.metadata.core.control;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

/** Durable authority for governance bindings keyed by {@code table_id}. */
public interface TableControlGovernanceBindingStore {

    GovernanceBinding save(GovernanceBinding binding);

    Optional<GovernanceBinding> find(UUID tableId, GovernanceBindingType type,
            String bindingRef);

    List<GovernanceBinding> list(UUID tableId);

    void delete(UUID tableId, GovernanceBindingType type, String bindingRef);

    void deleteAll(UUID tableId);
}

package org.pangrova.table.api.policy;

import org.pangrova.table.api.enums.GovernanceBindingType;

import org.pangrova.common.api.annotation.Internal;
import org.pangrova.table.api.ownership.TableControlRecord;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/** Table-owned bindings used to persist and replay policies against stable table identities. */
@Internal
public interface TablePolicyBindings extends TablePolicyAuthority {
    /** Includes every live state; consumers must check eligibility for the requested operation. */
    List<TableControlRecord> listLive();

    /** Verifies that the captured active physical identity still has the same owner and name. */
    void requireReadOwner(TableControlRecord captured, String operation);

    Optional<GovernanceBinding> find(UUID tableId, GovernanceBindingType type, String reference);

    /** Checks the current stable identity before writing the binding for the captured owner. */
    GovernanceBinding bind(TableControlRecord captured, GovernanceBindingType type, String reference,
            Map<String, String> config, String actor);

    /** Deletes only the binding for the supplied stable identity, including after name reuse. */
    void unbind(UUID tableId, GovernanceBindingType type, String reference);
}

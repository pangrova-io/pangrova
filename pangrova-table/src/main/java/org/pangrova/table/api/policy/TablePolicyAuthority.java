package org.pangrova.table.api.policy;

import org.pangrova.table.api.control.TableControlRecord;
import org.pangrova.table.api.governancebinding.GovernanceBinding;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Table-owned identity facts that external policy stores (quality rules, masks,
 * row filters) must bind to.
 *
 * <p>Policy adapters live outside the table control plane and must not reach
 * into catalog registration or table-control services directly. This port is
 * the narrow surface they consume: stable {@code table_id}, lifecycle state,
 * current catalog deployment, and governance bindings.</p>
 */
public interface TablePolicyAuthority {

    /** Resolves the live control record for a name, if the table is controlled. */
    Optional<TableControlRecord> find(String catalog, String schema, String table);

    /** Resolves a control record by stable identity. */
    Optional<TableControlRecord> findById(UUID tableId);

    /**
     * Validates that policy may bind here: the table must be controlled, ACTIVE,
     * and deployed by the current catalog incarnation.
     */
    TableControlRecord requireBindable(String catalog, String schema, String table);

    /** Rejects a record whose catalog deployment is no longer current. */
    void requireCurrentCatalog(TableControlRecord record, String operation);

    /** Lists governance bindings captured for an already resolved identity. */
    List<GovernanceBinding> bindingsFor(UUID tableId);
}

package org.pangrova.persistence.workflow.api.security;

import org.pangrova.common.api.annotation.Experimental;

import java.util.List;
import java.util.UUID;

import org.pangrova.security.api.mask.ColumnMask;
import org.pangrova.security.api.rowfilter.RowPolicy;
import org.pangrova.table.api.policy.GovernanceBinding;

/**
 * Owner-supplied projections that bind named policies and governance bindings to the live stable
 * table identity before deferred authorization renders them.
 */
@Experimental
public interface TableControlPolicyProjections {

    List<RowPolicy> projectRows(UUID tableId, String catalog, String schema, String table,
            List<RowPolicy> rows);

    List<ColumnMask> projectColumns(UUID tableId, String catalog, String schema, String table,
            List<ColumnMask> masks, List<GovernanceBinding> bindings);
}

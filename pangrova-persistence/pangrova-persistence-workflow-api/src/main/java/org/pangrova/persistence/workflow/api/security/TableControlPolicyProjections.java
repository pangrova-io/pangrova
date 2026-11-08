package org.pangrova.persistence.workflow.api.security;

import java.util.List;
import java.util.UUID;

import org.pangrova.security.api.mask.ColumnMask;
import org.pangrova.security.api.rowfilter.RowPolicy;
import org.pangrova.table.api.policybinding.GovernanceBinding;

/**
 * Owner-supplied projections that bind named policies and governance bindings to the live stable
 * table identity before deferred authorization renders them.
 */
public interface TableControlPolicyProjections {

    List<RowPolicy> projectRows(UUID tableId, String catalog, String schema, String table,
            List<RowPolicy> rows);

    List<ColumnMask> projectColumns(UUID tableId, String catalog, String schema, String table,
            List<ColumnMask> masks, List<GovernanceBinding> bindings);
}

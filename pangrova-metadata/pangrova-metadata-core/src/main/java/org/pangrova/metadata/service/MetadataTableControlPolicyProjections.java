package org.pangrova.metadata.service;

import java.util.List;
import java.util.UUID;

import org.pangrova.metadata.security.TableControlColumnMaskStore;
import org.pangrova.persistence.workflow.api.security.TableControlPolicyProjections;
import org.pangrova.security.api.mask.ColumnMask;
import org.pangrova.security.api.rowfilter.RowPolicy;
import org.pangrova.table.api.policybinding.GovernanceBinding;

/** Binds row and column policy content to the live stable table identity owned by Metadata. */
public final class MetadataTableControlPolicyProjections implements TableControlPolicyProjections {

    @Override
    public List<RowPolicy> projectRows(UUID tableId, String catalog, String schema, String table,
            List<RowPolicy> rows) {
        return TableControlRowPolicyStore.project(tableId, catalog, schema, table, rows, List.of());
    }

    @Override
    public List<ColumnMask> projectColumns(UUID tableId, String catalog, String schema, String table,
            List<ColumnMask> masks, List<GovernanceBinding> bindings) {
        return TableControlColumnMaskStore.project(tableId, catalog, schema, table, masks, bindings);
    }
}

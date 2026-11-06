package org.pangrova.metadata.adapter.security;

import org.pangrova.table.api.ownership.TableControlRecord;
import org.pangrova.table.api.ownership.TableControlState;
import org.pangrova.table.api.policybinding.TablePolicyAuthority;
import org.pangrova.security.api.mask.ColumnMask;
import org.pangrova.persistence.security.api.mask.EnrichedColumnMaskStore;
import org.pangrova.security.api.error.AccessDeniedException;
import org.pangrova.security.api.mask.ColumnMaskRecord;

import java.util.List;
import java.util.Objects;
import java.util.UUID;

/** Binds policy persistence and actual foreground mask reads to the live stable table identity. */
public final class TableControlColumnMaskStore implements EnrichedColumnMaskStore {
    private final TablePolicyAuthority tables;
    private final EnrichedColumnMaskStore storage;

    public TableControlColumnMaskStore(TablePolicyAuthority tables, EnrichedColumnMaskStore storage) {
        this.tables = Objects.requireNonNull(tables, "tables");
        this.storage = Objects.requireNonNull(storage, "storage");
    }

    @Override
    public List<ColumnMask> getColumnMasks(String catalog, String schema, String table, String column) {
        return current(catalog, schema, table, storage.getColumnMasks(catalog, schema, table, column)).stream()
                .filter(mask -> mask.columnName().equals(column)).toList();
    }

    @Override
    public List<ColumnMask> getTableMasks(String catalog, String schema, String table) {
        return current(catalog, schema, table, storage.getTableMasks(catalog, schema, table));
    }

    @Override
    public List<ColumnMaskRecord> getMasks(UUID tableId, String catalog, String schema, String table, String column) {
        return storage.getMasks(tableId, catalog, schema, table, column);
    }

    @Override
    public void saveColumnMask(ColumnMask mask) {
        if (mask.maskName().startsWith("auto_pii_")) {
            throw new AccessDeniedException(
                    "Tag-derived masks are read from their stable tag authority; use SET/UNSET TAG");
        }
        TableControlRecord record = mask.tableId() == null
                ? tables.requireBindable(mask.catalogName(), mask.schemaName(), mask.tableName())
                : tables.find(mask.catalogName(), mask.schemaName(), mask.tableName()).orElseThrow(
                        () -> new AccessDeniedException("Mask target identity is no longer registered"));
        if (mask.tableId() != null && !record.tableId().equals(mask.tableId())) {
            throw new AccessDeniedException("Mask event belongs to a replaced table identity");
        }
        tables.requireCurrentCatalog(record, "Mask publication");
        if (record.state() != TableControlState.ACTIVE && record.state() != TableControlState.RENAMING) {
            throw new AccessDeniedException("Mask target is not ACTIVE or RENAMING");
        }
        storage.saveColumnMask(new ColumnMask(mask.maskId(), mask.maskName(), mask.catalogName(),
                mask.schemaName(), mask.tableName(), mask.columnName(), mask.maskExpression(),
                mask.applyToRoles(), mask.exemptRoles(), mask.enabled(), record.tableId()));
    }

    @Override
    public void deleteMask(long maskId) {
        if (maskId <= 0) throw new AccessDeniedException("Tag-derived masks must be changed through their tag");
        storage.deleteMask(maskId);
    }

    private List<ColumnMask> current(String catalog, String schema, String table, List<ColumnMask> masks) {
        if (masks.stream().anyMatch(mask -> mask.tableId() == null)) {
            throw new AccessDeniedException("Mask has no proven stable table binding; explicitly rebind the policy");
        }
        var record = tables.find(catalog, schema, table);
        if (record.isEmpty()) return List.of();
        var target = record.orElseThrow();
        tables.requireCurrentCatalog(target, "Mask read");
        if (target.state() != TableControlState.ACTIVE) {
            throw new AccessDeniedException("Mask read requires an ACTIVE controlled table");
        }
        return project(target.tableId(), catalog, schema, table, masks,
                tables.bindingsFor(target.tableId()));
    }
    /** Pure projection shared with durable publication's transaction-bound security observation. */
    public static List<ColumnMask> project(UUID tableId, String catalog, String schema, String table,
            List<ColumnMask> masks, List<org.pangrova.table.api.policybinding.GovernanceBinding> bindings) {
        if (masks.stream().anyMatch(mask -> mask.tableId() == null)) {
            throw new AccessDeniedException("Mask has no proven stable table binding; explicitly rebind the policy");
        }
        if (tableId == null) return List.of();
        var result = new java.util.ArrayList<ColumnMask>();
        masks.stream().filter(mask -> tableId.equals(mask.tableId()))
                .filter(mask -> !mask.maskName().startsWith("auto_pii_")).forEach(result::add);
        for (var binding : bindings) {
            if (binding.bindingType() != org.pangrova.table.api.policybinding.GovernanceBindingType.COLUMN_TAG
                    || !"pii_type".equals(binding.config().get("tagKey"))) continue;
            String column = Objects.requireNonNull(binding.config().get("column"), "Tag column");
            String expression = org.pangrova.governance.core.classification.TagBasedSecurityPolicy
                    .piiTypeToMaskExpression(binding.config().get("value"));
            if (expression == null) throw new AccessDeniedException("Unknown persisted PII tag policy");
            // Mandatory classification masks precede authored masks and cannot be deleted as a projection.
            result.add(new ColumnMask(-1, "auto_pii_" + column, catalog, schema, table, column,
                    expression, List.of(), List.of(), true, tableId));
        }
        return List.copyOf(result);
    }

}

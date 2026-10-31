package org.pangrova.metadata.service;

import org.pangrova.table.api.control.TableControlRecord;
import org.pangrova.table.api.control.TableControlState;
import org.pangrova.table.api.policy.TablePolicyAuthority;
import org.pangrova.security.api.rowfilter.IdentityRowPolicyStore;
import org.pangrova.security.api.rowfilter.RowPolicy;
import org.pangrova.security.api.rowfilter.RowPolicyStore;
import org.pangrova.security.api.AccessDeniedException;

import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Stream;

/** Reads and persists row policies under their stable table identity; names are diagnostic. */
public final class TableControlRowPolicyStore implements RowPolicyStore {
    private final TablePolicyAuthority tables;
    private final IdentityRowPolicyStore storage;

    public TableControlRowPolicyStore(TablePolicyAuthority tables, IdentityRowPolicyStore storage) {
        this.tables = Objects.requireNonNull(tables, "tables");
        this.storage = Objects.requireNonNull(storage, "storage");
    }

    @Override
    public List<RowPolicy> getPolicies(String catalog, String schema, String table) {
        var record = tables.find(catalog, schema, table);
        if (record.isPresent()) {
            var current = record.orElseThrow();
            tables.requireCurrentCatalog(current, "Row policy read");
            if (current.state() != TableControlState.ACTIVE) {
                throw new AccessDeniedException("Row policy read requires an ACTIVE controlled table");
            }
        }
        UUID tableId = record.map(TableControlRecord::tableId).orElse(null);
        return project(tableId, catalog, schema, table, storage.getPolicies(catalog, schema, table),
                tableId == null ? List.of() : storage.getPolicies(tableId));
    }

    @Override
    public void savePolicy(RowPolicy policy) {
        var current = tables.requireBindable(
                policy.catalogName(), policy.schemaName(), policy.tableName());
        if (policy.tableId() != null && !policy.tableId().equals(current.tableId())) {
            throw new AccessDeniedException("Row policy belongs to a replaced table identity");
        }
        storage.savePolicy(at(policy, current.tableId(), current.catalog(), current.schema(), current.table()));
    }

    @Override
    public void deletePolicy(long policyId) {
        storage.deletePolicy(policyId);
    }

    /** Pure projection shared with transaction-bound background authorization. */
    static List<RowPolicy> project(UUID tableId, String catalog, String schema, String table,
            List<RowPolicy> named, List<RowPolicy> bound) {
        if (named.stream().anyMatch(policy -> policy.tableId() == null)) {
            throw new AccessDeniedException("Row policy has no proven stable table binding; explicitly rebind it");
        }
        if (tableId == null) return List.of();
        return Stream.concat(named.stream(), bound.stream()).distinct()
                .filter(policy -> tableId.equals(policy.tableId()))
                .map(policy -> at(policy, tableId, catalog, schema, table)).toList();
    }

    private static RowPolicy at(RowPolicy policy, UUID tableId, String catalog, String schema, String table) {
        return new RowPolicy(policy.policyId(), policy.policyName(), catalog, schema, table,
                policy.filterExpression(), policy.applyToRoles(), policy.enabled(), policy.priority(), tableId);
    }
}

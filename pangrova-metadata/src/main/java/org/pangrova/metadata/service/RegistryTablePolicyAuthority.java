package org.pangrova.metadata.service;

import org.pangrova.metadata.core.catalog.registry.CatalogRegistry;
import org.pangrova.table.api.governancebinding.GovernanceBinding;
import org.pangrova.table.api.control.TableControlRecord;
import org.pangrova.table.api.policy.TablePolicyAuthority;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/**
 * Adapts the live catalog registry to {@link TablePolicyAuthority}. Delegation is
 * dynamic because the registry may replace its table control service during startup.
 */
public final class RegistryTablePolicyAuthority implements TablePolicyAuthority {

    private final CatalogRegistry catalogs;

    public RegistryTablePolicyAuthority(CatalogRegistry catalogs) {
        this.catalogs = Objects.requireNonNull(catalogs, "catalogs");
    }

    @Override
    public Optional<TableControlRecord> find(String catalog, String schema, String table) {
        return catalogs.tableControl().find(catalog, schema, table);
    }

    @Override
    public Optional<TableControlRecord> findById(UUID tableId) {
        return catalogs.tableControl().findById(tableId);
    }

    @Override
    public TableControlRecord requireBindable(String catalog, String schema, String table) {
        return catalogs.tableGovernance().requireBindable(catalog, schema, table);
    }

    @Override
    public void requireCurrentCatalog(TableControlRecord record, String operation) {
        catalogs.tableControl().requireCurrentCatalog(record, operation);
    }

    @Override
    public List<GovernanceBinding> bindingsFor(UUID tableId) {
        return catalogs.tableGovernance().bindingsFor(tableId);
    }
}

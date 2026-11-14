package org.pangrova.persistence.table.api.provider;

import org.pangrova.table.api.maintenance.TableControlMaintenanceStore;
import org.pangrova.table.api.ownership.TableControlStore;
import org.pangrova.table.api.policy.TableControlGovernanceBindingStore;
import org.pangrova.table.api.statistics.TableControlStatisticsStore;

import java.util.Objects;

/** Repositories sharing one configured persistence authority. */
public record TableStores(
        TableControlStore control,
        TableControlStatisticsStore statistics,
        TableControlGovernanceBindingStore governance,
        TableControlMaintenanceStore maintenance) {
    public TableStores {
        Objects.requireNonNull(control, "control");
        Objects.requireNonNull(statistics, "statistics");
        Objects.requireNonNull(governance, "governance");
        Objects.requireNonNull(maintenance, "maintenance");
    }
}

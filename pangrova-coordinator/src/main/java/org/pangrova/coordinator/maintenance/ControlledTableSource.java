package org.pangrova.coordinator.maintenance;

import org.pangrova.metadata.core.control.TableControlMaintenanceService;
import org.pangrova.metadata.core.control.TableControlRecord;
import org.pangrova.metadata.core.control.TableControlService;
import org.pangrova.metadata.core.control.TableControlState;
import org.pangrova.metadata.core.control.TableMaintenancePolicy;
import org.pangrova.streaming.maintenance.MaintenanceTableSource;

import java.util.Collection;
import java.util.List;
import java.util.Objects;

/**
 * Selects the tables an automatic maintenance scan may visit.
 *
 * <p>Only persisted control records are candidates: OBSERVED tables have no
 * ownership record and are never maintained. A LINKED table is included only
 * after its policy explicitly enables an operation; MANAGED tables inherit the
 * ownership defaults. Retired and quarantined records are excluded.</p>
 */
public final class ControlledTableSource implements MaintenanceTableSource {

    private final TableControlService control;
    private final TableControlMaintenanceService policies;

    public ControlledTableSource(TableControlService control,
            TableControlMaintenanceService policies) {
        this.control = Objects.requireNonNull(control, "control");
        this.policies = Objects.requireNonNull(policies, "policies");
    }

    @Override
    public Collection<String> listManagedTables() {
        if (!control.enabled()) {
            return List.of();
        }
        return control.listLive().stream()
                .filter(record -> record.state() == TableControlState.ACTIVE)
                .filter(this::hasEnabledOperation)
                .map(TableControlRecord::qualifiedName)
                .toList();
    }

    private boolean hasEnabledOperation(TableControlRecord record) {
        TableMaintenancePolicy policy = policies.policyFor(record);
        return !policy.enabledOperations().isEmpty();
    }
}

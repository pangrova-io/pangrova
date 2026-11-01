package org.pangrova.coordinator.maintenance;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.table.store.memory.control.InMemoryTableControlMaintenanceStore;
import org.pangrova.table.store.memory.control.InMemoryTableControlStore;
import org.pangrova.table.core.maintenance.TableControlMaintenanceService;
import org.pangrova.table.api.control.TableControlRecord;
import org.pangrova.table.core.control.TableControlService;
import org.pangrova.table.core.control.TableControlSettings;
import org.pangrova.table.api.control.TableControlState;
import org.pangrova.spi.connector.metadata.table.TableIdentity;
import org.pangrova.spi.connector.metadata.table.TableVersionToken;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class ControlledTableSourceTest {

    private InMemoryTableControlStore store;
    private TableControlService control;
    private TableControlMaintenanceService policies;
    private ControlledTableSource source;

    @BeforeEach
    void setUp() {
        store = new InMemoryTableControlStore();
        control = new TableControlService(store, name -> new org.pangrova.table.api.control.CatalogDeploymentIdentity("test-deployment", 1),
                new TableControlSettings(true, true, Set.of()));
        policies = new TableControlMaintenanceService(
                new InMemoryTableControlMaintenanceStore());
        source = new ControlledTableSource(control, policies);
    }

    @Test
    void managedTableUsesOwnershipDefaults() {
        control.registerCreated("cat", "s", "managed", "alice",
                Optional.of(identity("managed")), Optional.of(version()));

        assertThat(source.listManagedTables()).containsExactly("cat.s.managed");
    }

    @Test
    void linkedTableIsExcludedUntilItsPolicyEnablesAnOperation() {
        TableControlRecord record = adopt("linked");
        assertThat(source.listManagedTables()).isEmpty();

        policies.applyAdoptionProperties(record,
                Map.of("maintenance_operations", "COMPACT"), "alice");

        assertThat(source.listManagedTables()).containsExactly("cat.s.linked");
    }

    @Test
    void nonActiveRecordsAreExcluded() {
        TableControlRecord record = adopt("stale");
        policies.applyAdoptionProperties(record,
                Map.of("maintenance_operations", "COMPACT"), "alice");
        store.save(record.toBuilder().state(TableControlState.STALE)
                .metadataRevision(record.metadataRevision() + 1).build());

        assertThat(source.listManagedTables()).isEmpty();
    }

    @Test
    void disabledControlPlaneNeverSuppliesTables() {
        TableControlService disabled = new TableControlService(store, name -> new org.pangrova.table.api.control.CatalogDeploymentIdentity("test-deployment", 1),
                new TableControlSettings(false, false, Set.of()));
        disabled.registerCreated("cat", "s", "managed", "alice",
                Optional.of(identity("managed")), Optional.of(version()));

        assertThat(new ControlledTableSource(disabled, policies).listManagedTables())
                .isEmpty();
    }

    private TableControlRecord adopt(String table) {
        return control.adopt("cat", "s", table, "alice",
                Optional.of(identity(table)), Optional.of(version()));
    }

    private static TableIdentity identity(String table) {
        return new TableIdentity("ICEBERG", "uuid-" + table, "s3://bucket/" + table, "schema");
    }

    private static TableVersionToken version() {
        return TableVersionToken.of("ICEBERG", "uuid", "v1");
    }
}

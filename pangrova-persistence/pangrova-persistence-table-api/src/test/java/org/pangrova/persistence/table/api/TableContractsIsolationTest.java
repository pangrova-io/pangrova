package org.pangrova.persistence.table.api;

import org.junit.jupiter.api.Test;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.table.api.ownership.TableControlStore;
import org.pangrova.table.api.ownership.TableControlRecord;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TableContractsIsolationTest {
    @Test
    void sharedTableContractsLinkWithOnlyTheirContractClosure() throws Exception {
        URL[] artifacts = {
                org.pangrova.connector.api.metadata.table.QualifiedTableName.class.getProtectionDomain().getCodeSource().getLocation(),
                org.pangrova.types.Type.class.getProtectionDomain().getCodeSource().getLocation(),
                org.pangrova.batch.api.page.Page.class.getProtectionDomain().getCodeSource().getLocation(),

                PangrovaException.class.getProtectionDomain().getCodeSource().getLocation(),
                TableControlRecord.class.getProtectionDomain().getCodeSource().getLocation()
        };
        try (var loader = new URLClassLoader(artifacts, ClassLoader.getPlatformClassLoader())) {
            link(loader, List.of(
                "org.pangrova.table.api.identity.CatalogDeploymentIdentity",
                "org.pangrova.table.api.enums.ObservationOutcome",
                "org.pangrova.table.api.enums.OwnershipMode",
                "org.pangrova.table.api.enums.TableControlProvenance",
                "org.pangrova.table.api.ownership.TableControlRecord",
                "org.pangrova.table.api.rename.TableControlRenameIntent",
                "org.pangrova.table.api.enums.TableControlState",
                "org.pangrova.table.api.ownership.TableControlView",
                "org.pangrova.table.api.identity.TableRef",
                "org.pangrova.table.api.observation.TableVersionObservation",
                "org.pangrova.table.api.policy.GovernanceBinding",
                "org.pangrova.table.api.enums.GovernanceBindingType",
                "org.pangrova.table.api.enums.MaintenanceOperation",
                "org.pangrova.table.api.enums.MaintenanceRunState",
                "org.pangrova.table.api.maintenance.TableMaintenancePolicy",
                "org.pangrova.table.api.maintenance.TableMaintenanceRun",
                "org.pangrova.table.api.policy.TablePolicyAuthority",
                "org.pangrova.table.api.statistics.TableControlStatistics"));
            assertThat(loader.loadClass(TableControlStore.class.getName()).isInterface()).isTrue();
            assertNoRuntime(loader);
        }
    }

    @Test
    void storagePortsLinkWithoutBackendsOrDomainRuntime() throws Exception {
        URL[] artifacts = {
                org.pangrova.connector.api.metadata.table.QualifiedTableName.class.getProtectionDomain().getCodeSource().getLocation(),
                org.pangrova.types.Type.class.getProtectionDomain().getCodeSource().getLocation(),
                org.pangrova.batch.api.page.Page.class.getProtectionDomain().getCodeSource().getLocation(),

                PangrovaException.class.getProtectionDomain().getCodeSource().getLocation(),
                TableControlRecord.class.getProtectionDomain().getCodeSource().getLocation(),
                TableControlStore.class.getProtectionDomain().getCodeSource().getLocation()
        };
        try (var loader = new URLClassLoader(artifacts, ClassLoader.getPlatformClassLoader())) {
            link(loader, List.of(
                "org.pangrova.table.api.exception.TableControlException",
                "org.pangrova.table.api.ownership.TableControlStore",
                "org.pangrova.table.api.policy.TableControlGovernanceBindingStore",
                "org.pangrova.table.api.maintenance.TableControlMaintenanceStore",
                "org.pangrova.table.api.statistics.TableControlStatisticsStore"));
            var contract = loader.loadClass(TableControlStore.class.getName());
            assertThat(contract.getMethod("listVersionObservations", java.util.UUID.class, int.class).isDefault())
                    .as("each backend must implement version history").isFalse();
            assertNoRuntime(loader);
        }
    }

    private static void link(ClassLoader loader, List<String> names) throws Exception {
        for (String name : names) {
            var type = loader.loadClass(name);
            assertThat(type.getClassLoader()).isSameAs(loader);
            type.getMethods();
            type.getConstructors();
            type.getRecordComponents();
        }
    }

    private static void assertNoRuntime(ClassLoader loader) {
        for (String name : List.of("org.pangrova.metadata.catalog.registry.CatalogRegistry",
                "org.pangrova.server.coordinator.bootstrap.CoordinatorBootstrap",
                "org.pangrova.persistence.table.postgresql.control.JdbcTableControlStore",
                "org.pangrova.persistence.table.memory.control.InMemoryTableControlStore",
                "com.fasterxml.jackson.databind.ObjectMapper", "org.postgresql.Driver")) {
            assertThatThrownBy(() -> loader.loadClass(name)).isInstanceOf(ClassNotFoundException.class);
        }
    }
}

package org.pangrova.metadata.control;

import static org.pangrova.metadata.testinfra.TableStoreFixtures.memoryTableStores;

import org.pangrova.table.api.identity.CatalogDeploymentIdentity;
import org.pangrova.table.api.ownership.TableControlRecord;
import org.pangrova.table.ownership.TableControlService;
import org.pangrova.table.config.TableControlSettings;
import org.pangrova.persistence.table.postgresql.control.JdbcTableControlStore;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.catalog.registry.CatalogRegistry;
import org.pangrova.metadata.api.enums.CatalogState;
import org.pangrova.metadata.api.enums.CatalogType;
import org.pangrova.persistence.catalog.postgresql.catalog.PostgresCatalogDeploymentRepository;
import org.pangrova.testing.testkit.postgres.SharedPostgresContainer;
import org.pangrova.connector.api.metadata.table.TableIdentity;
import org.pangrova.security.api.exception.AccessDeniedException;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Durable deployment fencing, including a peer whose local handle has not been refreshed. */
@Tag("integration")
class TableControlCatalogDeploymentIntegrationTest {
    private static final TableIdentity IDENTITY =
            new TableIdentity("ICEBERG", "unchanged-uuid", "s3://bucket/table", "schema");
    private DataSource dataSource;
    private PostgresCatalogDeploymentRepository catalogs;

    @BeforeEach
    void setUp() throws SQLException {
        dataSource = SharedPostgresContainer.migratedDataSource("table_catalog_binding");
        catalogs = new PostgresCatalogDeploymentRepository(dataSource);
    }

    @Test
    void deploymentBindingSurvivesStoreReconstruction() {
        CatalogDescriptor descriptor = descriptor("original", 1);
        catalogs.saveCatalogDescriptor(descriptor);
        TableControlService original = control();
        TableControlRecord record = original.registrations().registerCreated("cat", "db", "t", "alice",
                Optional.of(IDENTITY), Optional.empty()).orElseThrow();

        TableControlService reconstructed = control();

        assertThat(reconstructed.find("cat", "db", "t")).contains(record);
        assertThat(record.catalogIncarnationId()).isEqualTo(descriptor.incarnationId());
        assertThat(record.catalogRevision()).isEqualTo(descriptor.revision());
        reconstructed.requireManagedForWrite("cat", "db", "t", "INSERT", Optional.of(IDENTITY));
    }

    @Test
    void readIdentityChecksCatalogOnceAndRejectsANewDeployment() {
        catalogs.saveCatalogDescriptor(descriptor("original", 1));
        var lookups = new AtomicInteger();
        TableControlService service = control(lookups);
        TableControlRecord record = service.registrations().registerCreated("cat", "db", "t", "alice",
                Optional.of(IDENTITY), Optional.empty()).orElseThrow();
        lookups.set(0);

        service.requireReadIdentity(record, Optional.of(IDENTITY), "SELECT");
        assertThat(lookups).hasValue(1);

        catalogs.saveCatalogDescriptor(descriptor("original", 2));
        assertThatThrownBy(() -> service.requireReadIdentity(record, Optional.of(IDENTITY), "SELECT"))
                .isInstanceOf(AccessDeniedException.class).hasMessageContaining("catalog deployment");
        assertThat(lookups).hasValue(2);
    }

    @Test
    void samePhysicalUuidCannotInheritControlAfterCatalogRecreation() {
        catalogs.saveCatalogDescriptor(descriptor("original", 1));
        TableControlService original = control();
        original.registrations().registerCreated("cat", "db", "t", "alice", Optional.of(IDENTITY), Optional.empty());
        catalogs.deleteCatalogDescriptor(catalogs.getCatalogDescriptor("cat").orElseThrow().identity());
        catalogs.saveCatalogDescriptor(descriptor("replacement", 1));

        assertThatThrownBy(() -> control().requireManagedForWrite(
                "cat", "db", "t", "INSERT", Optional.of(IDENTITY)))
                .isInstanceOf(AccessDeniedException.class).hasMessageContaining("catalog deployment");
    }

    @Test
    void staleLocalHandleCannotAuthorizeAPeerCatalogRevision() {
        CatalogRegistry registry = new CatalogRegistry(memoryTableStores(), catalogs);
        try {
            registry.register(descriptor("original", 1));
            registry.tableControl().registrations().registerCreated("cat", "db", "t", "alice",
                    Optional.of(IDENTITY), Optional.empty());
            catalogs.saveCatalogDescriptor(descriptor("original", 2));

            assertThat(registry.getCatalog("cat").getDescriptor().revision()).isEqualTo(1);
            assertThatThrownBy(() -> registry.tableControl().describe("cat", "db", "t"))
                    .isInstanceOf(AccessDeniedException.class).hasMessageContaining("reload the local catalog");
            assertThatThrownBy(() -> registry.tableControl().registrations().registerCreated(
                    "cat", "db", "new_table", "alice", Optional.of(IDENTITY), Optional.empty()))
                    .isInstanceOf(AccessDeniedException.class).hasMessageContaining("reload the local catalog");
        } finally {
            registry.shutdown();
        }
    }

    @Test
    void staleLocalHandleCannotAdoptAfterPeerCatalogRecreation() {
        CatalogRegistry registry = new CatalogRegistry(memoryTableStores(), catalogs);
        try {
            registry.register(descriptor("original", 1));
            catalogs.deleteCatalogDescriptor(catalogs.getCatalogDescriptor("cat").orElseThrow().identity());
            catalogs.saveCatalogDescriptor(descriptor("replacement", 1));

            assertThatThrownBy(() -> registry.tableControl().registrations().adopt(
                    "cat", "db", "external", "alice", Optional.of(IDENTITY), Optional.empty()))
                    .isInstanceOf(AccessDeniedException.class).hasMessageContaining("reload the local catalog");
            assertThat(registry.tableControl().find("cat", "db", "external")).isEmpty();
        } finally {
            registry.shutdown();
        }
    }

    private TableControlService control() {
        return control(new AtomicInteger());
    }

    private TableControlService control(AtomicInteger lookups) {
        return new TableControlService(new JdbcTableControlStore(dataSource),
                name -> {
                    lookups.incrementAndGet();
                    CatalogDescriptor deployment = new PostgresCatalogDeploymentRepository(dataSource)
                            .getCatalogDescriptor(name).orElseThrow();
                    return new CatalogDeploymentIdentity(deployment.incarnationId(), deployment.revision());
                },
                new TableControlSettings(true, true, Set.of()),
                Clock.fixed(Instant.parse("2026-10-25T00:00:00Z"), ZoneOffset.UTC));
    }

    private static CatalogDescriptor descriptor(String incarnation, long revision) {
        return new CatalogDescriptor("cat", CatalogType.INTERNAL, "test", CatalogState.ACTIVE,
                Map.of(), "", Set.of(), incarnation, revision);
    }
}

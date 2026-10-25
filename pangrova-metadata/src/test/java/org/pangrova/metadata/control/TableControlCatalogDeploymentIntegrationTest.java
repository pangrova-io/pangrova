package org.pangrova.metadata.control;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.pangrova.metadata.catalog.CatalogDescriptor;
import org.pangrova.metadata.catalog.CatalogRegistry;
import org.pangrova.metadata.catalog.CatalogState;
import org.pangrova.metadata.catalog.CatalogType;
import org.pangrova.metadata.store.JdbcMetadataStore;
import org.pangrova.metadata.testinfra.SharedPostgresContainer;
import org.pangrova.spi.connector.metadata.table.TableIdentity;
import org.pangrova.spi.security.AccessDeniedException;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Durable deployment fencing, including a peer whose local handle has not been refreshed. */
@Tag("integration")
class TableControlCatalogDeploymentIntegrationTest {
    private static final TableIdentity IDENTITY =
            new TableIdentity("ICEBERG", "unchanged-uuid", "s3://bucket/table", "schema");
    private DataSource dataSource;
    private JdbcMetadataStore catalogs;

    @BeforeEach
    void setUp() throws SQLException {
        dataSource = SharedPostgresContainer.migratedDataSource("table_catalog_binding");
        catalogs = new JdbcMetadataStore(dataSource);
    }

    @Test
    void deploymentBindingSurvivesStoreReconstruction() {
        CatalogDescriptor descriptor = descriptor("original", 1);
        catalogs.saveCatalogDescriptor(descriptor);
        TableControlService original = control();
        TableControlRecord record = original.registerCreated("cat", "db", "t", "alice",
                Optional.of(IDENTITY), Optional.empty()).orElseThrow();

        TableControlService reconstructed = control();

        assertThat(reconstructed.find("cat", "db", "t")).contains(record);
        assertThat(record.catalogIncarnationId()).isEqualTo(descriptor.incarnationId());
        assertThat(record.catalogRevision()).isEqualTo(descriptor.revision());
        reconstructed.requireManagedForWrite("cat", "db", "t", "INSERT", Optional.of(IDENTITY));
    }

    @Test
    void samePhysicalUuidCannotInheritControlAfterCatalogRecreation() {
        catalogs.saveCatalogDescriptor(descriptor("original", 1));
        TableControlService original = control();
        original.registerCreated("cat", "db", "t", "alice", Optional.of(IDENTITY), Optional.empty());
        catalogs.deleteCatalogDescriptor("cat");
        catalogs.saveCatalogDescriptor(descriptor("replacement", 1));

        assertThatThrownBy(() -> control().requireManagedForWrite(
                "cat", "db", "t", "INSERT", Optional.of(IDENTITY)))
                .isInstanceOf(AccessDeniedException.class).hasMessageContaining("catalog deployment");
    }

    @Test
    void staleLocalHandleCannotAuthorizeAPeerCatalogRevision() {
        CatalogRegistry registry = new CatalogRegistry(catalogs);
        try {
            registry.register(descriptor("original", 1));
            registry.tableControl().registerCreated("cat", "db", "t", "alice",
                    Optional.of(IDENTITY), Optional.empty());
            catalogs.saveCatalogDescriptor(descriptor("original", 2));

            assertThat(registry.getCatalog("cat").getDescriptor().revision()).isEqualTo(1);
            assertThatThrownBy(() -> registry.tableControl().describe("cat", "db", "t"))
                    .isInstanceOf(AccessDeniedException.class).hasMessageContaining("reload the local catalog");
            assertThatThrownBy(() -> registry.tableControl().registerCreated(
                    "cat", "db", "new_table", "alice", Optional.of(IDENTITY), Optional.empty()))
                    .isInstanceOf(AccessDeniedException.class).hasMessageContaining("reload the local catalog");
        } finally {
            registry.shutdown();
        }
    }

    @Test
    void staleLocalHandleCannotAdoptAfterPeerCatalogRecreation() {
        CatalogRegistry registry = new CatalogRegistry(catalogs);
        try {
            registry.register(descriptor("original", 1));
            catalogs.deleteCatalogDescriptor("cat");
            catalogs.saveCatalogDescriptor(descriptor("replacement", 1));

            assertThatThrownBy(() -> registry.tableControl().adopt(
                    "cat", "db", "external", "alice", Optional.of(IDENTITY), Optional.empty()))
                    .isInstanceOf(AccessDeniedException.class).hasMessageContaining("reload the local catalog");
            assertThat(registry.tableControl().find("cat", "db", "external")).isEmpty();
        } finally {
            registry.shutdown();
        }
    }

    private TableControlService control() {
        return new TableControlService(new JdbcTableControlStore(dataSource),
                name -> {
                    CatalogDescriptor deployment = new JdbcMetadataStore(dataSource)
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

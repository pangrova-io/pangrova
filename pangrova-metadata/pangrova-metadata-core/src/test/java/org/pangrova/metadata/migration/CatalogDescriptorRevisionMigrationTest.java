package org.pangrova.metadata.migration;

import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.MigrationVersion;
import org.junit.jupiter.api.Test;
import org.pangrova.persistence.catalog.postgresql.catalog.PostgresCatalogDeploymentRepository;
import org.pangrova.metadata.testinfra.SharedPostgresContainer;

import javax.sql.DataSource;

import static org.assertj.core.api.Assertions.assertThat;

class CatalogDescriptorRevisionMigrationTest {

    @Test
    void backfillsNonemptyV34CatalogAndPreservesExactSnapshotAfterUpdate() throws Exception {
        DataSource dataSource = SharedPostgresContainer.isolatedDataSource("catalog_history_upgrade");
        migrateTo(dataSource, "34");
        try (var connection = dataSource.getConnection();
                var statement = connection.prepareStatement("INSERT INTO catalog_definitions "
                        + "(name,type,provider,state,properties,comment,allowed_tenants,incarnation_id,revision) "
                        + "VALUES ('upgraded','INTERNAL','iceberg','ACTIVE',?::jsonb,'before',"
                        + "?::jsonb,'incarnation-before',7)")) {
            statement.setString(1, "{\"url\":\"old\"}");
            statement.setString(2, "[\"tenant-a\"]");
            statement.executeUpdate();
        }

        migrateTo(dataSource, "35");

        var store = new PostgresCatalogDeploymentRepository(dataSource);
        var historical = store.findCatalogDescriptorRevision("upgraded", "incarnation-before", 7);
        assertThat(historical).isPresent();
        assertThat(historical.get().properties()).containsEntry("url", "old");
        assertThat(historical.get().allowedTenants()).containsExactly("tenant-a");
        assertThat(store.findCatalogDescriptorRevision("upgraded", "incarnation-before", 6))
                .isEmpty();
        assertThat(store.findCatalogDescriptorRevision("upgraded", "another-incarnation", 7))
                .isEmpty();
    }

    private static void migrateTo(DataSource dataSource, String version) {
        Flyway.configure()
                .dataSource(dataSource)
                .locations("classpath:db/migration")
                .target(MigrationVersion.fromVersion(version))
                .load()
                .migrate();
    }
}

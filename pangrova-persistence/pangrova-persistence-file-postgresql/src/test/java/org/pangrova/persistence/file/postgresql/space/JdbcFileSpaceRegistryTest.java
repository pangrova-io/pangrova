package org.pangrova.persistence.file.postgresql.space;

import org.junit.jupiter.api.Test;
import org.pangrova.file.api.error.FileStorageException;
import org.pangrova.testing.testkit.postgres.MigratedPostgres;
import org.pangrova.file.api.space.FileSpace;
import org.pangrova.file.api.space.FileSpaceDefinition;
import org.pangrova.file.api.space.FileSpaceMutation;
import org.pangrova.file.api.space.FileSpaceType;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests the shared PostgreSQL authority behind the Volume and Stage surfaces. */
class JdbcFileSpaceRegistryTest {

    @Test
    void createIsIdempotentOnlyWhenRequestedAndKeepsFirstDefinition() throws Exception {
        var registry = registry();
        var managed = managedVolume("landing");

        FileSpace created = registry.create(managed, false);
        assertThat(created.external()).isFalse();
        assertThat(created.location()).isEmpty();
        assertThat(created.directoryEnabled()).isFalse();
        assertThat(created.qualifiedName()).isEqualTo("media.raw.landing");
        assertThat(created.createdAt()).isEqualTo(created.updatedAt());

        assertThatThrownBy(() -> registry.create(managed, false))
                .isInstanceOf(FileStorageException.class)
                .hasMessageContaining("VOLUME already exists");

        FileSpace repeated = registry.create(new FileSpaceDefinition(FileSpaceType.VOLUME,
                "media", "raw", "landing", false, "different-profile", "",
                false, "changed", "someone-else"), true);
        assertThat(repeated).isEqualTo(created);
    }

    @Test
    void volumeAndStageShareANameWithoutSharingANamespace() throws Exception {
        var registry = registry();
        FileSpace volume = registry.create(managedVolume("landing"), false);
        FileSpace stage = registry.create(managedStage("landing"), false);

        assertThat(volume.type()).isEqualTo(FileSpaceType.VOLUME);
        assertThat(stage.type()).isEqualTo(FileSpaceType.STAGE);
        assertThat(registry.list(FileSpaceType.VOLUME, "media", "raw")).containsExactly(volume);
        assertThat(registry.list(FileSpaceType.STAGE, "media", "raw")).containsExactly(stage);
        assertThat(registry.list(FileSpaceType.VOLUME, null, null)).containsExactly(volume);
    }

    @Test
    void externalDefinitionRequiresLocationAndProfileAndNormalizesLocation() throws Exception {
        var registry = registry();
        assertThatThrownBy(() -> registry.create(new FileSpaceDefinition(FileSpaceType.VOLUME,
                "media", "raw", "no-location", true, "prod-s3", "", false, "", "owner"),
                false))
                .isInstanceOf(FileStorageException.class)
                .hasMessageContaining("requires LOCATION/URL");
        assertThatThrownBy(() -> registry.create(new FileSpaceDefinition(FileSpaceType.VOLUME,
                "media", "raw", "no-profile", true, "", "s3://acme/raw", false, "", "owner"),
                false))
                .isInstanceOf(FileStorageException.class)
                .hasMessageContaining("requires a storage profile");
        assertThatThrownBy(() -> registry.create(new FileSpaceDefinition(FileSpaceType.VOLUME,
                "media", "raw", "managed-with-location", false, "", "s3://acme/raw",
                false, "", "owner"), false))
                .isInstanceOf(FileStorageException.class)
                .hasMessageContaining("cannot declare a location");

        FileSpace external = registry.create(externalVolume("landing", "s3://acme/raw/"),
                false);
        assertThat(external.location()).isEqualTo("s3://acme/raw");
        assertThat(external.storageProfile()).isEqualTo("prod-s3");
    }

    @Test
    void externalLocationsRejectTraversalAndOverlapAcrossBothSurfaces() throws Exception {
        var registry = registry();
        for (String bad : List.of("s3://acme/../secrets", "s3://acme/raw\\data",
                "s3://acme/raw /data", "s3://acme/raw/..", "acme/raw", "s3://")) {
            assertThatThrownBy(() -> registry.create(externalVolume("bad", bad), false))
                    .as(bad)
                    .isInstanceOf(FileStorageException.class);
        }

        registry.create(externalVolume("landing", "s3://acme/raw"), false);
        assertThatThrownBy(() -> registry.create(externalVolume("nested", "s3://acme/raw/data"),
                false))
                .isInstanceOf(FileStorageException.class)
                .hasMessageContaining("overlaps");
        assertThatThrownBy(() -> registry.create(externalStage("parent", "s3://acme"), false))
                .isInstanceOf(FileStorageException.class)
                .hasMessageContaining("overlaps");
        assertThat(registry.create(externalStage("sibling", "s3://acme/other"), false).name())
                .isEqualTo("sibling");
    }

    @Test
    void onlyStagesAcceptADirectoryProjection() throws Exception {
        var registry = registry();
        assertThatThrownBy(() -> registry.create(new FileSpaceDefinition(FileSpaceType.VOLUME,
                "media", "raw", "landing", false, "", "", true, "", "owner"), false))
                .isInstanceOf(FileStorageException.class)
                .hasMessageContaining("Only stages");

        FileSpace stage = registry.create(new FileSpaceDefinition(FileSpaceType.STAGE,
                "media", "raw", "landing", true, "prod-s3", "s3://acme/stage",
                true, "landing zone", "owner"), false);
        assertThat(stage.directoryEnabled()).isTrue();
        assertThatThrownBy(() -> registry.mutate(FileSpaceType.VOLUME, "media", "raw",
                "missing-volume", new FileSpaceMutation.SetDirectoryEnabled(true)))
                .isInstanceOf(FileStorageException.class)
                .hasMessageContaining("does not exist");
    }

    @Test
    void mutationsApplyOneFieldAndRejectIllegalChanges() throws Exception {
        var registry = registry();
        FileSpace original = registry.create(managedVolume("landing"), false);

        FileSpace commented = registry.mutate(FileSpaceType.VOLUME, "media", "raw", "landing",
                new FileSpaceMutation.SetComment("raw landing"));
        assertThat(commented.comment()).isEqualTo("raw landing");
        assertThat(commented.storageProfile()).isEqualTo(original.storageProfile());
        assertThat(commented.createdAt()).isEqualTo(original.createdAt());
        assertThat(commented.updatedAt()).isAfterOrEqualTo(original.updatedAt());

        FileSpace owned = registry.mutate(FileSpaceType.VOLUME, "media", "raw", "landing",
                new FileSpaceMutation.SetOwner("analytics"));
        assertThat(owned.owner()).isEqualTo("analytics");
        assertThat(owned.comment()).isEqualTo("raw landing");

        FileSpace reprofiled = registry.mutate(FileSpaceType.VOLUME, "media", "raw", "landing",
                new FileSpaceMutation.SetStorageProfile("archive-s3"));
        assertThat(reprofiled.storageProfile()).isEqualTo("archive-s3");

        assertThatThrownBy(() -> registry.mutate(FileSpaceType.VOLUME, "media", "raw",
                "landing", new FileSpaceMutation.SetLocation("s3://acme/raw")))
                .isInstanceOf(FileStorageException.class)
                .hasMessageContaining("cannot declare a location");
        assertThatThrownBy(() -> registry.mutate(FileSpaceType.VOLUME, "media", "raw",
                "landing", new FileSpaceMutation.SetDirectoryEnabled(true)))
                .isInstanceOf(FileStorageException.class)
                .hasMessageContaining("Only stages");

        assertThat(registry.find(FileSpaceType.VOLUME, "media", "raw", "landing"))
                .contains(reprofiled);
    }

    @Test
    void externalLocationCanBeMovedButNotOntoAnExistingPrefix() throws Exception {
        var registry = registry();
        registry.create(externalVolume("first", "s3://acme/first"), false);
        registry.create(externalVolume("second", "s3://acme/second"), false);

        FileSpace moved = registry.mutate(FileSpaceType.VOLUME, "media", "raw", "second",
                new FileSpaceMutation.SetLocation("s3://acme/moved/"));
        assertThat(moved.location()).isEqualTo("s3://acme/moved");

        assertThatThrownBy(() -> registry.mutate(FileSpaceType.VOLUME, "media", "raw", "second",
                new FileSpaceMutation.SetLocation("s3://acme/first/nested")))
                .isInstanceOf(FileStorageException.class)
                .hasMessageContaining("overlaps");

        assertThat(registry.mutate(FileSpaceType.VOLUME, "media", "raw", "second",
                new FileSpaceMutation.SetLocation("s3://acme/moved")).location())
                .isEqualTo("s3://acme/moved");
    }

    @Test
    void renameKeepsIdentityAndRejectsCollisions() throws Exception {
        var registry = registry();
        FileSpace original = registry.create(managedVolume("landing"), false);
        registry.create(managedVolume("staging"), false);

        FileSpace renamed = registry.rename(FileSpaceType.VOLUME, "media", "raw", "landing",
                "landed");
        assertThat(renamed.name()).isEqualTo("landed");
        assertThat(renamed.createdAt()).isEqualTo(original.createdAt());
        assertThat(registry.find(FileSpaceType.VOLUME, "media", "raw", "landing")).isEmpty();
        assertThat(registry.find(FileSpaceType.VOLUME, "media", "raw", "landed"))
                .contains(renamed);
        assertThatThrownBy(() -> registry.rename(FileSpaceType.VOLUME, "media", "raw",
                "landed", "staging"))
                .isInstanceOf(FileStorageException.class)
                .hasMessageContaining("already exists");
        assertThatThrownBy(() -> registry.rename(FileSpaceType.VOLUME, "media", "raw",
                "missing", "other"))
                .isInstanceOf(FileStorageException.class)
                .hasMessageContaining("does not exist");
    }

    @Test
    void dropHonoursIfExistsAndNeverTouchesTheOtherSurface() throws Exception {
        var registry = registry();
        registry.create(managedVolume("landing"), false);
        registry.create(managedStage("landing"), false);

        assertThat(registry.drop(FileSpaceType.VOLUME, "media", "raw", "landing", false))
                .isTrue();
        assertThat(registry.find(FileSpaceType.VOLUME, "media", "raw", "landing")).isEmpty();
        assertThat(registry.find(FileSpaceType.STAGE, "media", "raw", "landing")).isPresent();
        assertThat(registry.drop(FileSpaceType.VOLUME, "media", "raw", "landing", true))
                .isFalse();
        assertThatThrownBy(() -> registry.drop(FileSpaceType.VOLUME, "media", "raw", "landing",
                false))
                .isInstanceOf(FileStorageException.class)
                .hasMessageContaining("VOLUME does not exist");
    }

    @Test
    void listFiltersByCatalogAndSchema() throws Exception {
        var registry = registry();
        FileSpace raw = registry.create(managedVolume("landing", "media", "raw"), false);
        FileSpace curated = registry.create(managedVolume("landing", "media", "curated"),
                false);
        FileSpace otherCatalog = registry.create(managedVolume("landing", "other", "raw"),
                false);

        assertThat(registry.list(FileSpaceType.VOLUME, "media", null))
                .containsExactly(curated, raw);
        assertThat(registry.list(FileSpaceType.VOLUME, "media", "raw")).containsExactly(raw);
        assertThat(registry.list(FileSpaceType.VOLUME, null, null))
                .containsExactly(curated, raw, otherCatalog);
        assertThat(registry.list(FileSpaceType.STAGE, null, null)).isEmpty();
    }

    private static JdbcFileSpaceRegistry registry() throws Exception {
        return new JdbcFileSpaceRegistry(MigratedPostgres.create());
    }

    @Test
    void managedStorageRootSurvivesRenameAndRestartButNeverNameReuse() throws Exception {
        var dataSource = MigratedPostgres.create();
        var registry = new JdbcFileSpaceRegistry(dataSource);
        for (FileSpaceType type : FileSpaceType.values()) {
            var definition = type == FileSpaceType.VOLUME ? managedVolume("landing") : managedStage("landing");
            FileSpace original = registry.create(definition, false);
            assertThat(original.managedRoot()).startsWith("managed/");
            registry.rename(type, "media", "raw", "landing", "renamed");
            var restarted = new JdbcFileSpaceRegistry(dataSource);
            FileSpace renamed = restarted.find(type, "media", "raw", "renamed").orElseThrow();
            assertThat(renamed.managedRoot()).isEqualTo(original.managedRoot());
            FileSpace reused = restarted.create(definition, false);
            assertThat(reused.managedRoot()).isNotEqualTo(original.managedRoot());
            assertThat(restarted.create(definition, true).managedRoot()).isEqualTo(reused.managedRoot());
            restarted.drop(type, "media", "raw", "landing", false);
            assertThat(restarted.create(definition, false).managedRoot())
                    .isNotIn(original.managedRoot(), reused.managedRoot());
        }
    }

    @Test
    void upgradePreservesLegacyStorageRootsWithoutMovingExistingFiles() throws Exception {
        var dataSource = org.pangrova.testing.testkit.postgres.SharedPostgres.isolatedDataSource();
        org.flywaydb.core.Flyway.configure().dataSource(dataSource)
                .locations("classpath:db/migration").target("95").load().migrate();
        try (var connection = dataSource.getConnection();
                var insert = connection.prepareStatement("INSERT INTO file_space_registry "
                        + "(file_space_id, space_type, catalog_name, schema_name, space_name, is_external, "
                        + "storage_profile, location, owner_name, created_at, updated_at) "
                        + "VALUES (?, ?, 'media', 'raw', ?, ?, 'prod-s3', ?, 'owner', now(), now())")) {
            for (FileSpaceType type : FileSpaceType.values()) {
                for (boolean external : List.of(false, true)) {
                    insert.setObject(1, java.util.UUID.randomUUID());
                    insert.setString(2, type.name());
                    insert.setString(3, external ? "external" : "landing");
                    insert.setBoolean(4, external);
                    insert.setString(5, external ? "s3://acme/" + type : "");
                    insert.executeUpdate();
                }
            }
        }
        org.flywaydb.core.Flyway.configure().dataSource(dataSource)
                .locations("classpath:db/migration").load().migrate();
        var registry = new JdbcFileSpaceRegistry(dataSource);
        for (FileSpaceType type : FileSpaceType.values()) {
            String legacyRoot = type.name().toLowerCase(java.util.Locale.ROOT) + "s/media/raw/landing";
            assertThat(registry.find(type, "media", "raw", "landing").orElseThrow().managedRoot())
                    .isEqualTo(legacyRoot);
            assertThat(registry.rename(type, "media", "raw", "landing", "renamed").managedRoot())
                    .isEqualTo(legacyRoot);
            assertThat(registry.find(type, "media", "raw", "external").orElseThrow().managedRoot())
                    .isEmpty();
        }
    }

    private static FileSpaceDefinition managedVolume(String name) {
        return managedVolume(name, "media", "raw");
    }

    private static FileSpaceDefinition managedVolume(String name, String catalog,
            String schema) {
        return new FileSpaceDefinition(FileSpaceType.VOLUME, catalog, schema, name,
                false, "prod-s3", "", false, "", "owner");
    }

    private static FileSpaceDefinition externalVolume(String name, String location) {
        return new FileSpaceDefinition(FileSpaceType.VOLUME, "media", "raw", name,
                true, "prod-s3", location, false, "", "owner");
    }

    private static FileSpaceDefinition managedStage(String name) {
        return new FileSpaceDefinition(FileSpaceType.STAGE, "media", "raw", name,
                false, "prod-s3", "", false, "", "owner");
    }

    private static FileSpaceDefinition externalStage(String name, String location) {
        return new FileSpaceDefinition(FileSpaceType.STAGE, "media", "raw", name,
                true, "prod-s3", location, false, "", "owner");
    }
}

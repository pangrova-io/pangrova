package org.pangrova.testing.integration.e2e.file;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.persistence.file.postgresql.space.JdbcFileSpaceRegistry;
import org.pangrova.security.policy.builtin.authorization.DynamicRbacAccessControl;
import org.pangrova.security.api.enums.Privilege;
import org.pangrova.security.api.policy.PrivilegeGrant;
import org.pangrova.security.api.policy.Role;
import org.pangrova.security.api.policy.UserRoleAssignment;
import org.pangrova.security.mask.ColumnMaskingProcessor;
import org.pangrova.security.rowfilter.RowLevelSecurityFilter;
import org.pangrova.persistence.security.postgresql.mask.JdbcColumnMaskStore;
import org.pangrova.persistence.security.postgresql.policy.JdbcGrantStore;
import org.pangrova.persistence.security.postgresql.policy.JdbcRoleStore;
import org.pangrova.persistence.security.postgresql.rowfilter.JdbcRowPolicyStore;
import org.pangrova.file.api.enums.FileSpaceType;
import org.pangrova.testing.testkit.E2ETest;
import org.pangrova.testing.integration.e2e.PangrovaE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;

import static org.assertj.core.api.Assertions.assertThat;

/** Authenticated SQL -> parser/dispatch -> PostgreSQL authority -> client results. */
@E2ETest
class FileSpaceDdlE2ETest extends PangrovaE2ETestBase {
    private static final String CATALOG = "file_space_catalog";
    private static final String SCHEMA = "default";
    private static final String PREFIX = CATALOG + "." + SCHEMA + ".";

    @TempDir
    Path files;
    private String name;
    private String role;
    private JdbcRoleStore roles;
    private JdbcGrantStore grants;
    private JdbcFileSpaceRegistry registry;

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA,
                icebergWarehouse().toString()));
    }

    @BeforeEach
    void authority(TestInfo info) {
        name = "files_" + info.getTestMethod().orElseThrow().getName().toLowerCase(java.util.Locale.ROOT);
        role = name + "_role";
        roles = new JdbcRoleStore(stateStoreDataSource());
        assertThat(roles.createRole(Role.of(role))).isTrue();
        roles.grantRoleToUser(UserRoleAssignment.of(authenticatedCatalogSession(CATALOG).getUser(), role));
        grants = new JdbcGrantStore(stateStoreDataSource());
        grant(Privilege.CREATE_VOLUME, "VOLUME");
        grant(Privilege.READ_VOLUME, "VOLUME");
        grant(Privilege.MANAGE_VOLUME, "VOLUME");
        grant(Privilege.CREATE_STAGE, "STAGE");
        grant(Privilege.USAGE_STAGE, "STAGE");
        grant(Privilege.READ_STAGE, "STAGE");
        grant(Privilege.OWNER, "STAGE");
        registry = new JdbcFileSpaceRegistry(stateStoreDataSource());
        installAuthority();
    }

    private void installAuthority() {
        configureFeatureSecurity(new DynamicRbacAccessControl(grants, roles,
                new RowLevelSecurityFilter(new JdbcRowPolicyStore(stateStoreDataSource())),
                new ColumnMaskingProcessor(new JdbcColumnMaskStore(stateStoreDataSource()))));
    }

    @AfterEach
    void cleanup() {
        try {
            grant(Privilege.MANAGE_VOLUME, "VOLUME");
            grant(Privilege.OWNER, "STAGE");
            for (String suffix : List.of("", "_renamed", "_denied")) {
                executeDdl("DROP VOLUME IF EXISTS " + PREFIX + name + suffix);
                executeDdl("DROP STAGE IF EXISTS " + PREFIX + name + suffix);
            }
        } finally {
            try {
                roles.dropRole(role);
            } finally {
                clearFeatureSecurity();
            }
        }
    }

    @Test
    void independentNamespacesPersistThroughDispatcherRebuild() {
        String qualified = PREFIX + name;
        executeDdl("CREATE VOLUME " + qualified + " COMMENT 'volume comment'");
        executeDdl("CREATE STAGE " + qualified + " COMMENT = 'stage comment'");
        assertThat(executeQuery("DESCRIBE VOLUME " + qualified))
                .anyMatch(row -> "volume comment".equals(row.getRaw("comment")));
        assertThat(executeQuery("DESCRIBE STAGE " + qualified))
                .anyMatch(row -> "stage comment".equals(row.getRaw("comment")));

        executeDdl("ALTER VOLUME " + qualified + " SET COMMENT 'changed'");
        executeDdl("ALTER STAGE " + qualified + " SET DIRECTORY = (ENABLE = TRUE)");
        installAuthority();
        assertThat(registry.find(FileSpaceType.VOLUME, CATALOG, SCHEMA, name).orElseThrow().comment())
                .isEqualTo("changed");
        assertThat(executeQuery("SHOW VOLUMES IN " + CATALOG + "." + SCHEMA))
                .anyMatch(row -> name.equals(row.getRaw("name")));
        assertThat(executeQuery("SHOW STAGES IN " + CATALOG + "." + SCHEMA))
                .anyMatch(row -> name.equals(row.getRaw("name")));
        executeDdl("ALTER VOLUME " + qualified + " RENAME TO " + name + "_renamed");
        executeDdl("DROP STAGE " + qualified);
        assertThat(registry.find(FileSpaceType.STAGE, CATALOG, SCHEMA, name)).isEmpty();
        assertThat(registry.find(FileSpaceType.VOLUME, CATALOG, SCHEMA, name + "_renamed"))
                .isPresent();
    }

    @Test
    void externalCreationRequiresDedicatedPrivilegeAndDropRetainsBytes() throws Exception {
        Path source = files.resolve("source.bin");
        Files.writeString(source, "externally owned bytes");
        String ddl = "CREATE EXTERNAL VOLUME " + PREFIX + name
                + " LOCATION '" + files.toUri() + "' STORAGE_PROFILE 'external-files'";
        assertThat(executeExpectingError(ddl)).contains("CREATE_EXTERNAL_VOLUME");
        assertThat(registry.find(FileSpaceType.VOLUME, CATALOG, SCHEMA, name)).isEmpty();
        grant(Privilege.CREATE_EXTERNAL_VOLUME, "VOLUME");
        executeDdl(ddl);
        executeDdl("DROP VOLUME " + PREFIX + name);
        assertThat(Files.readString(source)).isEqualTo("externally owned bytes");

        executeDdl("CREATE EXTERNAL STAGE " + PREFIX + name
                + " URL = '" + files.toUri() + "' STORAGE_PROFILE = 'external-files'");
        executeDdl("DROP STAGE " + PREFIX + name);
        assertThat(Files.readString(source)).isEqualTo("externally owned bytes");
    }

    @Test
    void revocationHidesMetadataAndRejectsMutationBeforePersistence() {
        executeDdl("CREATE VOLUME " + PREFIX + name);
        grants.revokeGrants(role, Privilege.CREATE_VOLUME, CATALOG, SCHEMA, "*");
        grants.revokeGrants(role, Privilege.READ_VOLUME, CATALOG, SCHEMA, "*");
        grants.revokeGrants(role, Privilege.MANAGE_VOLUME, CATALOG, SCHEMA, "*");
        assertThat(executeExpectingError("CREATE VOLUME " + PREFIX + name + "_denied"))
                .contains("CREATE_VOLUME");
        assertThat(executeExpectingError("ALTER VOLUME " + PREFIX + name + " SET COMMENT 'forged'"))
                .contains("MANAGE_VOLUME");
        assertThat(executeExpectingError("DROP VOLUME " + PREFIX + name)).contains("MANAGE_VOLUME");
        assertThat(executeQuery("SHOW VOLUMES IN " + CATALOG + "." + SCHEMA)).isEmpty();
        assertThat(registry.find(FileSpaceType.VOLUME, CATALOG, SCHEMA, name).orElseThrow().comment())
                .isEmpty();
        assertThat(registry.find(FileSpaceType.VOLUME, CATALOG, SCHEMA, name + "_denied")).isEmpty();
    }

    @Test
    void refreshPublishesWorkerMetadataAndSubpathDeletionThroughSql() throws Exception {
        configureDurableStorageProfiles(true);
        var profiles = new org.pangrova.persistence.file.postgresql.profile.JdbcStorageProfileRegistry(stateStoreDataSource());
        String profile = name + "_profile";
        assertThat(profiles.createIfAbsent(new org.pangrova.filesystem.api.profile.StorageProfile(profile,
                org.pangrova.common.enums.StorageType.LOCAL, files.toString(), "", "", "",
                java.util.Map.of(), java.util.Map.of()))).isTrue();
        try {
            executeDdl("CREATE STAGE " + PREFIX + name + " STORAGE_PROFILE = '" + profile
                    + "' DIRECTORY = (ENABLE = TRUE)");
            var stage = registry.find(FileSpaceType.STAGE, CATALOG, SCHEMA, name).orElseThrow();
            Path root = files.resolve(org.pangrova.file.api.space.FileSpaceLayout.managedBaseKey(stage));
            Files.createDirectories(root.resolve("images"));
            Files.writeString(root.resolve("images/old.txt"), "old");
            var modified = java.time.Instant.parse("2001-02-03T04:05:06Z");
            Files.setLastModifiedTime(root.resolve("images/old.txt"),
                    java.nio.file.attribute.FileTime.from(modified));
            Files.writeString(root.resolve("keep.txt"), "keep");
            executeDdl("ALTER STAGE " + PREFIX + name + " REFRESH");
            var index = new org.pangrova.persistence.file.postgresql.directory.JdbcFileDirectoryIndex(stateStoreDataSource());
            assertThat(index.page(stage, null, 100)).extracting(org.pangrova.filesystem.api.object.FileStat::path)
                    .containsExactly("images/old.txt", "keep.txt");
            assertThat(index.page(stage, null, 100).get(0).size()).isEqualTo(3);
            assertThat(index.page(stage, null, 100).get(0).lastModified()).isEqualTo(modified);
            assertThat(index.page(stage, null, 100).get(0).md5()).isNull();
            assertThat(index.page(stage, null, 100).get(0).etag()).hasSize(64);

            Files.delete(root.resolve("images/old.txt"));
            Files.writeString(root.resolve("images/new.txt"), "new bytes");
            executeDdl("ALTER STAGE " + PREFIX + name + " REFRESH SUBPATH = 'images/'");
            installAuthority();
            assertThat(index.page(stage, null, 100)).extracting(org.pangrova.filesystem.api.object.FileStat::path)
                    .containsExactly("images/new.txt", "keep.txt");
            grants.revokeGrants(role, Privilege.OWNER, CATALOG, SCHEMA, "*");
            Files.writeString(root.resolve("unauthorized.txt"), "outside new snapshot");
            assertThat(executeExpectingError("ALTER STAGE " + PREFIX + name + " REFRESH"))
                    .contains("OWNER");
            assertThat(index.page(stage, null, 100)).hasSize(2);
        } finally {
            profiles.unregister(profile);
            configureDurableStorageProfiles(false);
        }
    }

    private void grant(Privilege privilege, String type) {
        grants.addGrant(PrivilegeGrant.of(role, privilege, type, CATALOG, SCHEMA, "*", null));
    }

    @Test
    void managedRenameKeepsFilesAndNameReuseCannotExposeThem() throws Exception {
        configureDurableStorageProfiles(true);
        var profiles = new org.pangrova.persistence.file.postgresql.profile.JdbcStorageProfileRegistry(stateStoreDataSource());
        String profile = name + "_profile";
        assertThat(profiles.createIfAbsent(new org.pangrova.filesystem.api.profile.StorageProfile(profile,
                org.pangrova.common.enums.StorageType.LOCAL, files.toString(), "", "", "",
                java.util.Map.of(), java.util.Map.of()))).isTrue();
        String original = PREFIX + name;
        String renamed = original + "_renamed";
        String options = " STORAGE_PROFILE = '" + profile + "'";
        try {
            executeDdl("CREATE STAGE " + original + options);
            var stage = registry.find(FileSpaceType.STAGE, CATALOG, SCHEMA, name).orElseThrow();
            Path root = files.resolve(stage.managedRoot());
            Files.createDirectories(root);
            Files.writeString(root.resolve("kept.txt"), "original namespace bytes");
            assertThat(executeQuery("LIST @" + original)).hasSize(1);
            executeDdl("ALTER STAGE " + original + " RENAME TO " + name + "_renamed");
            installAuthority();
            assertThat(executeQuery("LIST @" + renamed))
                    .singleElement().satisfies(row -> assertThat(row.getRaw("name"))
                            .isEqualTo("@" + renamed + "/kept.txt"));
            Files.writeString(root.resolve("remove.txt"), "remove only this object");
            grant(Privilege.WRITE_STAGE, "STAGE");
            assertThat(executeQuery("REMOVE @" + renamed + "/remove.txt"))
                    .singleElement().satisfies(row -> assertThat(row.getRaw("result")).isEqualTo("removed"));
            assertThat(root.resolve("remove.txt")).doesNotExist();
            assertThat(root.resolve("kept.txt")).exists();
            executeDdl("CREATE STAGE " + original + options);
            assertThat(executeQuery("LIST @" + original)).isEmpty();
            executeDdl("DROP STAGE " + renamed);
            executeDdl("CREATE STAGE " + renamed + options);
            assertThat(executeQuery("LIST @" + renamed)).isEmpty();
            assertThat(Files.readString(root.resolve("kept.txt"))).isEqualTo("original namespace bytes");
        } finally {
            profiles.unregister(profile);
            configureDurableStorageProfiles(false);
        }
    }
}

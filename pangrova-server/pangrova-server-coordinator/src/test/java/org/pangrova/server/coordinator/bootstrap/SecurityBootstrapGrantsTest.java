package org.pangrova.server.coordinator.bootstrap;

import org.junit.jupiter.api.Test;
import org.pangrova.coordinator.testinfra.SharedPostgresContainer;
import org.pangrova.query.dispatch.StatementExecutionContext;
import org.pangrova.coordinator.query.execution.dispatch.task.storage.ShowStorageProfilesTask;
import org.pangrova.query.api.session.QuerySession;
import org.pangrova.persistence.file.postgresql.profile.JdbcStorageProfileRegistry;
import org.pangrova.common.exception.BootstrapFailedException;
import org.pangrova.security.api.exception.AuthorizationDeniedException;
import org.pangrova.security.api.policy.GroupRoleMapping;
import org.pangrova.security.api.enums.Privilege;
import org.pangrova.security.api.policy.PrivilegeGrant;
import org.pangrova.security.api.policy.Role;
import org.pangrova.security.api.policy.UserRoleAssignment;
import org.pangrova.persistence.security.postgresql.policy.JdbcGrantStore;
import org.pangrova.persistence.security.postgresql.policy.JdbcRoleStore;
import org.pangrova.parser.api.ast.statement.ShowStorageProfilesStatement;
import org.pangrova.security.api.identity.UserIdentity;
import org.pangrova.security.api.exception.AccessDeniedException;
import org.pangrova.filesystem.api.profile.StorageProfile;
import org.pangrova.common.enums.StorageType;
import org.pangrova.testing.testkit.ContainerTestBase;

import java.time.Instant;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Production bootstrap reads the shared PostgreSQL role and grant authority on every check. */
class SecurityBootstrapGrantsTest extends ContainerTestBase {
    @Test
    void droppedProfileRevokesReadGrantBeforeSameNameRecreation() throws Exception {
        var source = SharedPostgresContainer.migratedDataSource("profile_read_recreation");
        var roles = new JdbcRoleStore(source);
        roles.createRole(Role.of("reader"));
        roles.grantRoleToUser(UserRoleAssignment.of("alice", "reader"));
        var grants = new JdbcGrantStore(source);
        var registry = new JdbcStorageProfileRegistry(source);
        registry.createIfAbsent(new StorageProfile("incoming", StorageType.S3,
                "old-bucket", "", "", "", Map.of(), Map.of()));
        grants.addGrant(PrivilegeGrant.of("reader", Privilege.READ_STORAGE_PROFILE,
                "STORAGE_PROFILE", "incoming", null, null, null));
        var bootstrap = new SecurityBootstrap(config(), source, org.pangrova.persistence.security.postgresql.mask.JdbcColumnMaskStore::new,
                    org.pangrova.persistence.security.postgresql.rowfilter.JdbcRowPolicyStore::new);
        try {
            bootstrap.start();
            var alice = user("alice", Set.of());
            assertThatCode(() -> bootstrap.accessControl().checkCanReadStorageProfile(alice, "incoming"))
                    .doesNotThrowAnyException();
            assertThat(registry.unregister("incoming")).isTrue();
            assertThat(registry.createIfAbsent(new StorageProfile("incoming", StorageType.S3,
                    "new-bucket", "", "", "", Map.of(), Map.of()))).isTrue();
            assertThatThrownBy(() -> bootstrap.accessControl().checkCanReadStorageProfile(alice, "incoming"))
                    .isInstanceOf(AuthorizationDeniedException.class);
        } finally {
            bootstrap.shutdown();
        }
    }

    @Test
    void profileVisibilityReadsPersistentGrantsAndPropagatesAuthorityOutage() throws Exception {
        var source = SharedPostgresContainer.migratedDataSource("profile_visibility");
        var roles = new JdbcRoleStore(source);
        roles.createRole(Role.of("profile-viewer"));
        roles.grantRoleToUser(UserRoleAssignment.of("alice", "profile-viewer"));
        var grants = new JdbcGrantStore(source);
        var registry = new JdbcStorageProfileRegistry(source);
        assertThat(registry.createIfAbsent(new StorageProfile("visible", StorageType.S3,
                "visible-bucket", "", "", "", Map.of(), Map.of()))).isTrue();
        assertThat(registry.createIfAbsent(new StorageProfile("hidden", StorageType.S3,
                "hidden-bucket", "", "", "", Map.of(), Map.of()))).isTrue();
        grants.addGrant(PrivilegeGrant.of("profile-viewer", Privilege.VIEW_STORAGE_PROFILE,
                "STORAGE_PROFILE", "visible", null, null, null));
        var bootstrap = new SecurityBootstrap(config(), source, org.pangrova.persistence.security.postgresql.mask.JdbcColumnMaskStore::new,
                    org.pangrova.persistence.security.postgresql.rowfilter.JdbcRowPolicyStore::new);
        try {
            bootstrap.start();
            var session = new QuerySession("profile-show", Map.of(), user("alice", Set.of()), "tenant-a", java.util.Optional.empty(), new org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory().capture());
            var execution = new StatementExecutionContext(null, session.queryId(), session);
            var task = new ShowStorageProfilesTask(registry, bootstrap.accessControl());
            var show = new ShowStorageProfilesStatement();
            assertThatThrownBy(() -> new ShowStorageProfilesTask(registry, null).execute(show, execution))
                    .isInstanceOf(AccessDeniedException.class);
            var anonymous = new StatementExecutionContext(null, "anonymous", null);
            assertThatThrownBy(() -> task.execute(show, anonymous))
                    .isInstanceOf(AccessDeniedException.class);
            assertThat(task.execute(show, execution).rows())
                    .extracting(row -> row.getRaw("name")).containsExactly("visible");
            grants.revokeGrants("profile-viewer", Privilege.VIEW_STORAGE_PROFILE, "visible", null, null);
            assertThat(task.execute(show, execution).rows()).isEmpty();
            grants.addGrant(PrivilegeGrant.of("profile-viewer", Privilege.MANAGE_STORAGE_PROFILE,
                    "STORAGE_PROFILE", "visible", null, null, null));
            assertThat(task.execute(show, execution).rows())
                    .extracting(row -> row.getRaw("name")).containsExactly("visible");
            assertThatThrownBy(() -> bootstrap.accessControl()
                    .checkCanReadStorageProfile(user("alice", Set.of()), "visible"))
                    .isInstanceOf(AuthorizationDeniedException.class);
            grants.addGrant(PrivilegeGrant.of("profile-viewer", Privilege.READ_STORAGE_PROFILE,
                    "STORAGE_PROFILE", "visible", null, null, null));
            assertThatCode(() -> bootstrap.accessControl()
                    .checkCanReadStorageProfile(user("alice", Set.of()), "visible"))
                    .doesNotThrowAnyException();
            assertThatThrownBy(() -> bootstrap.accessControl()
                    .checkCanReadStorageProfile(user("alice", Set.of()), "hidden"))
                    .isInstanceOf(AuthorizationDeniedException.class);
            grants.revokeGrants("profile-viewer", Privilege.READ_STORAGE_PROFILE, "visible", null, null);
            assertThatThrownBy(() -> bootstrap.accessControl()
                    .checkCanReadStorageProfile(user("alice", Set.of()), "visible"))
                    .isInstanceOf(AuthorizationDeniedException.class);
            try (var connection = source.getConnection(); var statement = connection.createStatement()) {
                statement.execute("ALTER TABLE security_grants RENAME TO unavailable_profile_grants");
            }
            assertThatThrownBy(() -> task.execute(show, execution))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("Failed listing all grants");
            assertThatThrownBy(() -> bootstrap.accessControl()
                    .checkCanReadStorageProfile(user("alice", Set.of()), "visible"))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("Failed listing all grants");
        } finally {
            bootstrap.shutdown();
        }
    }

    @Test
    void directRolesAndExplicitGroupMappingsReadCurrentGrantsAcrossCoordinators() throws Exception {
        var source = SharedPostgresContainer.migratedDataSource("rbac_bootstrap");
        var roles = new JdbcRoleStore(source);
        roles.createRole(Role.of("reader"));
        roles.grantRoleToUser(UserRoleAssignment.of("alice", "reader"));
        roles.mapGroupToRole(GroupRoleMapping.of("directory-analysts", "reader"));
        var grants = new JdbcGrantStore(source);
        grants.addGrant(PrivilegeGrant.of("reader", Privilege.SELECT, "TABLE", "lake", "public", "facts", null));
        var first = new SecurityBootstrap(config(), source, org.pangrova.persistence.security.postgresql.mask.JdbcColumnMaskStore::new,
                    org.pangrova.persistence.security.postgresql.rowfilter.JdbcRowPolicyStore::new);
        var second = new SecurityBootstrap(config(), source, org.pangrova.persistence.security.postgresql.mask.JdbcColumnMaskStore::new,
                    org.pangrova.persistence.security.postgresql.rowfilter.JdbcRowPolicyStore::new);
        try {
            first.start();
            second.start();
            var direct = user("alice", Set.of());
            var mapped = user("bob", Set.of("directory-analysts"));
            assertThatCode(() -> first.accessControl().checkCanSelectFromTable(direct, "lake", "public", "facts"))
                    .doesNotThrowAnyException();
            assertThatCode(() -> second.accessControl().checkCanSelectFromTable(mapped, "lake", "public", "facts"))
                    .doesNotThrowAnyException();
            assertThatThrownBy(() -> second.accessControl().checkCanSelectFromTable(
                    user("mallory", Set.of("reader")), "lake", "public", "facts"))
                    .isInstanceOf(AuthorizationDeniedException.class);
            grants.revokeGrants("reader", Privilege.SELECT, "lake", "public", "facts");
            assertThatThrownBy(() -> first.accessControl().checkCanSelectFromTable(direct, "lake", "public", "facts"))
                    .isInstanceOf(AuthorizationDeniedException.class);
            assertThatThrownBy(() -> second.accessControl().checkCanSelectFromTable(mapped, "lake", "public", "facts"))
                    .isInstanceOf(AuthorizationDeniedException.class);
        } finally {
            first.shutdown();
            second.shutdown();
        }
    }

    @Test
    void missingDatasourceFailsStartupAndClearsAuthentication() {
        var bootstrap = new SecurityBootstrap(config(), null, org.pangrova.persistence.security.postgresql.mask.JdbcColumnMaskStore::new,
                    org.pangrova.persistence.security.postgresql.rowfilter.JdbcRowPolicyStore::new);
        assertThatThrownBy(bootstrap::start).isInstanceOf(BootstrapFailedException.class)
                .hasRootCauseMessage("Security requires the authoritative PostgreSQL datasource");
        assertThat(bootstrap.isHealthy()).isFalse();
        assertThat(bootstrap.authenticationManager()).isNull();
        assertThat(bootstrap.accessControl()).isNull();
    }

    @Test
    void storeReadFailureCannotRetainAnEarlierGrant() throws Exception {
        var source = SharedPostgresContainer.migratedDataSource("rbac_read_failure");
        var roles = new JdbcRoleStore(source);
        roles.createRole(Role.of("reader"));
        roles.grantRoleToUser(UserRoleAssignment.of("alice", "reader"));
        new JdbcGrantStore(source).addGrant(
                PrivilegeGrant.of("reader", Privilege.SELECT, "TABLE", "lake", "public", "facts", null));
        var bootstrap = new SecurityBootstrap(config(), source, org.pangrova.persistence.security.postgresql.mask.JdbcColumnMaskStore::new,
                    org.pangrova.persistence.security.postgresql.rowfilter.JdbcRowPolicyStore::new);
        try {
            bootstrap.start();
            var alice = user("alice", Set.of());
            assertThatCode(() -> bootstrap.accessControl().checkCanSelectFromTable(alice, "lake", "public", "facts"))
                    .doesNotThrowAnyException();
            try (var connection = source.getConnection(); var statement = connection.createStatement()) {
                statement.execute("ALTER TABLE security_grants RENAME TO unavailable_grants");
            }
            assertThatThrownBy(() -> bootstrap.accessControl().checkCanSelectFromTable(
                    alice, "lake", "public", "facts")).isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("Failed listing all grants");
        } finally {
            bootstrap.shutdown();
        }
    }

    @Test
    void securityDisabledHasNoAuthority() {
        var bootstrap = new SecurityBootstrap(CoordinatorBootstrapConfig.withDefaults("unsecured", 0), null, org.pangrova.persistence.security.postgresql.mask.JdbcColumnMaskStore::new,
                    org.pangrova.persistence.security.postgresql.rowfilter.JdbcRowPolicyStore::new);
        bootstrap.start();
        assertThat(bootstrap.accessControl()).isNull();
    }

    private static UserIdentity user(String principal, Set<String> groups) {
        return new UserIdentity(principal, groups, Map.of(), Instant.now());
    }

    private static CoordinatorBootstrapConfig config() {
        return new CoordinatorBootstrapConfig(
                new CoordinatorBootstrapConfig.CoreConfig("secured", 0, 30_000, org.pangrova.server.coordinator.enums.CoordinatorDeploymentRole.COMBINED, "0.0.0.0"),
                new CoordinatorBootstrapConfig.FeatureFlags(false, true, false, false, false, false, false, false),
                CoordinatorBootstrapConfig.QueryConfig.defaults(),
                CoordinatorBootstrapConfig.TransportConfig.defaults(),
                CoordinatorBootstrapConfig.WorkerHealthConfig.defaults(), null,
                CoordinatorBootstrapConfig.SecurityConfig.noop(), null);
    }
}

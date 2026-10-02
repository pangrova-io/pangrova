package org.pangrova.coordinator.bootstrap;

import org.junit.jupiter.api.Test;
import org.pangrova.coordinator.testinfra.SharedPostgresContainer;
import org.pangrova.operations.bootstrap.BootstrapFailedException;
import org.pangrova.security.authz.AuthorizationDeniedException;
import org.pangrova.security.authz.GroupRoleMapping;
import org.pangrova.security.authz.Privilege;
import org.pangrova.security.authz.PrivilegeGrant;
import org.pangrova.security.authz.Role;
import org.pangrova.security.authz.UserRoleAssignment;
import org.pangrova.security.store.PersistentGrantStore;
import org.pangrova.security.store.PersistentRoleStore;
import org.pangrova.spi.security.UserIdentity;
import org.pangrova.testing.ContainerTestBase;

import java.time.Instant;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Production bootstrap reads the shared PostgreSQL role and grant authority on every check. */
class SecurityBootstrapGrantsTest extends ContainerTestBase {
    @Test
    void directRolesAndExplicitGroupMappingsReadCurrentGrantsAcrossCoordinators() throws Exception {
        var source = SharedPostgresContainer.isolatedDataSource("rbac_bootstrap");
        var roles = new PersistentRoleStore(source);
        roles.initSchema();
        roles.createRole(Role.of("reader"));
        roles.grantRoleToUser(UserRoleAssignment.of("alice", "reader"));
        roles.mapGroupToRole(GroupRoleMapping.of("directory-analysts", "reader"));
        var grants = new PersistentGrantStore(source);
        grants.addGrant(PrivilegeGrant.of("reader", Privilege.SELECT, "TABLE", "lake", "public", "facts", null));
        var first = new SecurityBootstrap(config(), source);
        var second = new SecurityBootstrap(config(), source);
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
        var bootstrap = new SecurityBootstrap(config(), null);
        assertThatThrownBy(bootstrap::start).isInstanceOf(BootstrapFailedException.class)
                .hasRootCauseMessage("Security requires the authoritative PostgreSQL datasource");
        assertThat(bootstrap.isHealthy()).isFalse();
        assertThat(bootstrap.authenticationManager()).isNull();
        assertThat(bootstrap.accessControl()).isNull();
    }

    @Test
    void storeReadFailureCannotRetainAnEarlierGrant() throws Exception {
        var source = SharedPostgresContainer.isolatedDataSource("rbac_read_failure");
        var roles = new PersistentRoleStore(source);
        roles.initSchema();
        roles.createRole(Role.of("reader"));
        roles.grantRoleToUser(UserRoleAssignment.of("alice", "reader"));
        new PersistentGrantStore(source).addGrant(
                PrivilegeGrant.of("reader", Privilege.SELECT, "TABLE", "lake", "public", "facts", null));
        var bootstrap = new SecurityBootstrap(config(), source);
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
        var bootstrap = new SecurityBootstrap(CoordinatorBootstrapConfig.withDefaults("unsecured", 0), null);
        bootstrap.start();
        assertThat(bootstrap.accessControl()).isNull();
    }

    private static UserIdentity user(String principal, Set<String> groups) {
        return new UserIdentity(principal, groups, Map.of(), Instant.now());
    }

    private static CoordinatorBootstrapConfig config() {
        return new CoordinatorBootstrapConfig(
                new CoordinatorBootstrapConfig.CoreConfig("secured", 0, 30_000),
                new CoordinatorBootstrapConfig.FeatureFlags(false, true, false, false, false, false, false, false),
                CoordinatorBootstrapConfig.QueryConfig.defaults(),
                CoordinatorBootstrapConfig.TransportConfig.defaults(),
                CoordinatorBootstrapConfig.WorkerHealthConfig.defaults(), null,
                CoordinatorBootstrapConfig.SecurityConfig.noop(), null);
    }
}

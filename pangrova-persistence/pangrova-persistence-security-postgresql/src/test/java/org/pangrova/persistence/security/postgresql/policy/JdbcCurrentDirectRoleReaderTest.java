package org.pangrova.persistence.security.postgresql.policy;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.security.api.error.AuthorizationDeniedException;
import org.pangrova.security.api.policy.GroupRoleMapping;
import org.pangrova.security.api.policy.Role;
import org.pangrova.security.api.policy.UserRoleAssignment;
import org.pangrova.testing.testkit.ContainerTestBase;
import org.pangrova.testing.testkit.postgres.MigratedPostgres;

import javax.sql.DataSource;
import java.sql.SQLException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JdbcCurrentDirectRoleReaderTest extends ContainerTestBase {
    private final JdbcCurrentDirectRoleReader reader = new JdbcCurrentDirectRoleReader();
    private DataSource source;
    private JdbcRoleStore roles;

    @BeforeEach
    void setUp() throws SQLException {
        source = MigratedPostgres.create();
        roles = new JdbcRoleStore(source);
        roles.createRole(Role.of("analyst"));
        roles.grantRoleToUser(UserRoleAssignment.of("alice", "analyst"));
    }

    @Test
    void readsUncommittedRevocationWithoutEndingTheCallersTransaction() throws Exception {
        try (var caller = source.getConnection(); var observer = source.getConnection()) {
            caller.setAutoCommit(false);
            try (var statement = caller.createStatement()) {
                assertThat(statement.executeUpdate("DELETE FROM pangrova_user_roles WHERE user_id='alice'"))
                        .isOne();
            }
            assertThatThrownBy(() -> reader.requireCurrentRoles(caller, "alice"))
                    .isInstanceOf(AuthorizationDeniedException.class);
            assertThat(reader.requireCurrentRoles(observer, "alice")).containsExactly("analyst");
            assertThat(caller.getAutoCommit()).isFalse();
            caller.rollback();
            assertThat(reader.requireCurrentRoles(caller, "alice")).containsExactly("analyst");
            caller.rollback();
        }
    }

    @Test
    void groupMappingAndRecreatedRoleCannotRestoreRevokedDirectAssignments() throws Exception {
        roles.mapGroupToRole(GroupRoleMapping.of("alice", "analyst"));
        roles.revokeRoleFromUser("alice", "analyst");
        try (var connection = source.getConnection()) {
            assertThatThrownBy(() -> reader.requireCurrentRoles(connection, "alice"))
                    .isInstanceOf(AuthorizationDeniedException.class);
            roles.grantRoleToUser(UserRoleAssignment.of("alice", "analyst"));
            roles.dropRole("analyst");
            roles.createRole(Role.of("analyst"));
            assertThatThrownBy(() -> reader.requireCurrentRoles(connection, "alice"))
                    .isInstanceOf(AuthorizationDeniedException.class);
        }
    }

    @Test
    void storageFailureIsNotClassifiedAsMissingAuthority() throws Exception {
        try (var connection = source.getConnection()) {
            connection.setAutoCommit(false);
            try (var statement = connection.createStatement()) {
                statement.executeUpdate("ALTER TABLE pangrova_user_roles RENAME TO unavailable_assignments");
            }
            assertThatThrownBy(() -> reader.requireCurrentRoles(connection, "alice"))
                    .isInstanceOf(SQLException.class).isNotInstanceOf(AuthorizationDeniedException.class);
            connection.rollback();
            assertThat(reader.requireCurrentRoles(connection, "alice")).containsExactly("analyst");
            connection.rollback();
        }
    }
}

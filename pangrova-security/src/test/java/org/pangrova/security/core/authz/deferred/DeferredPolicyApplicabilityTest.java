package org.pangrova.security.core.authz.deferred;

import org.pangrova.persistence.jdbc.security.rowfilter.JdbcRowPolicyStore;

import org.pangrova.persistence.jdbc.security.policy.JdbcRoleStore;

import org.pangrova.persistence.jdbc.security.policy.JdbcGrantStore;

import org.pangrova.persistence.jdbc.security.mask.JdbcColumnMaskStore;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.pangrova.spi.security.AuthorizationDeniedException;
import org.pangrova.spi.security.authz.DeferredAuthorizationScope;
import org.pangrova.security.core.authz.DynamicRbacAccessControl;
import org.pangrova.spi.security.policy.GroupRoleMapping;
import org.pangrova.spi.security.policy.Privilege;
import org.pangrova.spi.security.policy.PrivilegeGrant;
import org.pangrova.spi.security.policy.Role;
import org.pangrova.spi.security.policy.UserRoleAssignment;
import org.pangrova.spi.security.mask.ColumnMask;
import org.pangrova.security.core.mask.ColumnMaskingProcessor;
import org.pangrova.security.core.rowfilter.RowLevelSecurityFilter;
import org.pangrova.spi.security.rowfilter.RowPolicy;
import org.pangrova.testing.testkit.postgres.SharedPostgres;
import org.pangrova.spi.connector.metadata.table.QualifiedTableName;
import org.pangrova.spi.security.UserIdentity;
import org.pangrova.testing.testkit.ContainerTestBase;

import javax.sql.DataSource;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real PostgreSQL group-role policies cannot disappear when a persisted subject loses its group context. */
class DeferredPolicyApplicabilityTest extends ContainerTestBase {
    private static final QualifiedTableName TABLE = new QualifiedTableName("lake", "default", "orders");
    private DataSource source;
    private JdbcRoleStore roles;
    private JdbcRowPolicyStore rows;
    private JdbcColumnMaskStore masks;
    private DynamicRbacAccessControl access;

    @BeforeEach
    void setup() throws Exception {
        source = SharedPostgres.isolatedDataSource();
        roles = new JdbcRoleStore(source);
        roles.initSchema();
        var grants = new JdbcGrantStore(source);
        rows = new JdbcRowPolicyStore(source);
        masks = new JdbcColumnMaskStore(source);
        roles.createRole(Role.of("reader"));
        roles.createRole(Role.of("restricted"));
        roles.grantRoleToUser(UserRoleAssignment.of("alice", "reader"));
        roles.mapGroupToRole(GroupRoleMapping.of("private-group", "restricted"));
        for (var privilege : List.of(Privilege.SELECT, Privilege.ALTER)) {
            grants.addGrant(PrivilegeGrant.of("reader", privilege, "TABLE", "lake", "default", "orders", null));
        }
        access = new DynamicRbacAccessControl(grants, roles,
                new RowLevelSecurityFilter(rows), new ColumnMaskingProcessor(masks));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void rawReplayRejectsUnknownApplicabilityAndTransformedReplayUsesProvenDirectRoles(boolean rowPolicy)
            throws Exception {
        var raw = DeferredAuthorizationScope.raw(TABLE, Set.of("email"), TABLE);
        requireCurrent(raw);
        if (rowPolicy) {
            rows.savePolicy(new RowPolicy(0, "private_rows", "lake", "default", "orders",
                    "email IS NOT NULL", List.of("restricted"), true, 1, null));
        } else {
            masks.saveColumnMask(new ColumnMask(0, "private_email", "lake", "default", "orders", "email",
                    "NULL", List.of("restricted"), List.of(), true, null));
        }
        var authenticated = new UserIdentity("alice", Set.of("private-group"), Map.of(), Instant.now());
        var filter = access.getRowFilter(authenticated, "lake", "default", "orders");
        var mask = access.getColumnMask(authenticated, "lake", "default", "orders", "email");
        assertThat(rowPolicy ? filter : mask).isPresent();
        var transformed = new DeferredAuthorizationScope(List.of(
                new DeferredAuthorizationScope.TablePermission(TABLE, Set.of("email"), Privilege.SELECT,
                        DeferredAuthorizationScope.ReadMode.SECURITY_TRANSFORMED,
                        new DeferredAuthorizationScope.PolicySnapshot(filter, Map.of("email", mask))),
                new DeferredAuthorizationScope.TablePermission(TABLE, Set.of(), Privilege.ALTER,
                        DeferredAuthorizationScope.ReadMode.NONE, DeferredAuthorizationScope.PolicySnapshot.empty())),
                Optional.empty());

        assertThatThrownBy(() -> requireCurrent(raw)).isInstanceOf(AuthorizationDeniedException.class)
                .hasMessageContaining("role applicability");
        assertThatThrownBy(() -> requireCurrent(transformed)).isInstanceOf(AuthorizationDeniedException.class)
                .hasMessageContaining("role applicability");

        roles.grantRoleToUser(UserRoleAssignment.of("alice", "restricted"));
        requireCurrent(transformed);
        assertThatThrownBy(() -> requireCurrent(raw)).isInstanceOf(AuthorizationDeniedException.class)
                .hasMessageContaining("Raw artifact reads");
    }

    private void requireCurrent(DeferredAuthorizationScope scope) throws Exception {
        try (var connection = source.getConnection()) {
            connection.setAutoCommit(false);
            try {
                DeferredAuthorization.requireCurrent(connection, "alice", scope);
            } finally {
                connection.rollback();
            }
        }
    }
}

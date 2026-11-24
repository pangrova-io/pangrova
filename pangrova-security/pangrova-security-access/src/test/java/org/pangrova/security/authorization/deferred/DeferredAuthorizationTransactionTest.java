package org.pangrova.security.authorization.deferred;

import org.junit.jupiter.api.Test;
import org.pangrova.connector.api.metadata.table.QualifiedTableName;
import org.pangrova.persistence.jdbc.api.security.policy.CurrentPolicyReader;
import org.pangrova.security.api.authorization.DeferredAuthorizationScope;
import org.pangrova.security.api.authorization.DeferredAuthorizationScope.PolicySnapshot;
import org.pangrova.security.api.authorization.DeferredAuthorizationScope.TablePermission;
import org.pangrova.security.api.exception.AuthorizationDeniedException;
import org.pangrova.security.api.mask.ColumnMask;
import org.pangrova.security.api.enums.Privilege;
import org.pangrova.security.api.policy.PrivilegeGrant;
import org.pangrova.security.api.rowfilter.RowPolicy;

import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Protects the transaction and fail-closed order when backends are injected. */
class DeferredAuthorizationTransactionTest {
    private static final QualifiedTableName TABLE = new QualifiedTableName("lake", "public", "orders");
    private static final DeferredAuthorizationScope SCOPE =
            DeferredAuthorizationScope.raw(TABLE, Set.of("email"), TABLE);
    private static final Set<String> ROLES = Set.of("reader");

    @Test
    void authorityAndPolicyUseTheSameBorrowedTransactionInOrder() throws Exception {
        var connection = borrowedConnection();
        var calls = new ArrayList<String>();
        var authorization = new DeferredAuthorization(c -> {
            assertThat(c).isSameAs(connection);
            calls.add("lock");
        }, (c, principal) -> {
            assertThat(c).isSameAs(connection);
            assertThat(principal).isEqualTo("alice");
            calls.add("roles");
            return ROLES;
        }, (c, roles) -> {
            assertThat(c).isSameAs(connection);
            assertThat(roles).isEqualTo(ROLES);
            calls.add("grants");
            return grants();
        });
        authorization.requireCurrent(connection, "alice", SCOPE, (c, principal, roles, permission) -> {
            assertThat(c).isSameAs(connection);
            assertThat(principal).isEqualTo("alice");
            assertThat(roles).isEqualTo(ROLES);
            assertThat(permission.privilege()).isEqualTo(Privilege.SELECT);
            calls.add("policy");
            return PolicySnapshot.empty();
        });
        // The ALTER permission is checked against grants, without an irrelevant policy read.
        assertThat(calls).containsExactly("lock", "roles", "grants", "policy");
    }

    @Test
    void lockFailureDoesNotReadStaleAuthorityOrReplaceTheFailure() {
        var failure = new SQLException("publication lock unavailable");
        var authorization = new DeferredAuthorization(c -> { throw failure; },
                (c, p) -> { throw new AssertionError("roles read before lock"); },
                (c, r) -> { throw new AssertionError("grants read before lock"); });
        assertThatThrownBy(() -> authorization.requireCurrent(borrowedConnection(), "alice", SCOPE,
                (c, p, r, permission) -> { throw new AssertionError("policy read before lock"); }))
                .isSameAs(failure);
    }

    @Test
    void revokedPermissionStopsBeforePolicyProjection() {
        var authorization = new DeferredAuthorization(c -> {}, (c, p) -> ROLES, (c, r) -> List.of());
        assertThatThrownBy(() -> authorization.requireCurrent(borrowedConnection(), "alice", SCOPE,
                (c, p, r, permission) -> { throw new AssertionError("policy read after denial"); }))
                .isInstanceOf(AuthorizationDeniedException.class).hasMessageContaining("revoked");
    }

    @Test
    void absentAuthenticatedSubjectStopsBeforeAcquiringLock() {
        var authorization = new DeferredAuthorization(c -> { throw new AssertionError("lock acquired"); },
                (c, p) -> ROLES, (c, r) -> grants());
        assertThatThrownBy(() -> authorization.requireCurrent(borrowedConnection(), " ", SCOPE,
                (c, p, r, permission) -> PolicySnapshot.empty()))
                .isInstanceOf(AuthorizationDeniedException.class).hasMessageContaining("authenticated subject");
    }

    @Test
    void unstableRowPolicyStopsBeforeReadingMasks() {
        var calls = new ArrayList<String>();
        var reader = new DeferredPolicyReader(new CurrentPolicyReader() {
            @Override
            public List<RowPolicy> readRows(Connection connection, TablePermission permission) {
                calls.add("rows");
                return List.of(new RowPolicy(1, "groups", "lake", "public", "orders",
                        "current_groups() IS NOT NULL", List.of("reader"), true, 1, null));
            }

            @Override
            public List<ColumnMask> readMasks(Connection connection, TablePermission permission) {
                throw new AssertionError("mask read after unstable row policy");
            }
        });
        assertThatThrownBy(() -> reader.observe(borrowedConnection(), "alice", ROLES, SCOPE.permissions().get(0)))
                .isInstanceOf(AuthorizationDeniedException.class);
        assertThat(calls).containsExactly("rows");
    }

    @Test
    void requestedMaskColumnsShareOneReadOnTheBorrowedTransaction() throws Exception {
        var connection = borrowedConnection();
        var columns = Set.of("email", "phone");
        var scope = DeferredAuthorizationScope.raw(TABLE, columns, TABLE);
        var calls = new ArrayList<String>();
        var reader = new DeferredPolicyReader(new CurrentPolicyReader() {
            @Override
            public List<RowPolicy> readRows(Connection supplied, TablePermission permission) {
                assertThat(supplied).isSameAs(connection);
                calls.add("rows");
                return List.of();
            }

            @Override
            public List<ColumnMask> readMasks(Connection supplied, TablePermission permission) {
                assertThat(supplied).isSameAs(connection);
                assertThat(permission.columns()).isEqualTo(columns);
                calls.add("masks");
                return List.of(new ColumnMask(1, "hidden", "lake", "public", "orders",
                        "email", "NULL", List.of("reader"), List.of(), true, null));
            }
        });

        assertThat(reader.observe(connection, "alice", ROLES, scope.permissions().get(0)).columnMasks())
                .isEqualTo(Map.of("email", Optional.of("NULL"), "phone", Optional.empty()));
        assertThat(calls).containsExactly("rows", "masks");
    }

    @Test
    void absentPolicySubjectIsRejectedBeforeAnyDatabaseRead() {
        var reader = new DeferredPolicyReader(new CurrentPolicyReader() {
            @Override
            public List<RowPolicy> readRows(Connection connection, TablePermission permission) {
                throw new AssertionError("row read before subject validation");
            }

            @Override
            public List<ColumnMask> readMasks(Connection connection, TablePermission permission) {
                throw new AssertionError("mask read before subject validation");
            }
        });
        assertThatThrownBy(() -> reader.observe(borrowedConnection(), "", ROLES, SCOPE.permissions().get(0)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static List<PrivilegeGrant> grants() {
        return List.of(PrivilegeGrant.of("reader", Privilege.SELECT, "TABLE", "lake", "public", "orders", null),
                PrivilegeGrant.of("reader", Privilege.ALTER, "TABLE", "lake", "public", "orders", null));
    }

    /** Any access, including commit/rollback/close, would violate the Core's borrowed ownership. */
    private static Connection borrowedConnection() {
        return (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(), new Class<?>[]{Connection.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("toString")) return "borrowed-transaction";
                    throw new AssertionError("Core touched the borrowed connection: " + method.getName());
                });
    }
}

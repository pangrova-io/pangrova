package org.pangrova.security.core.authz;

import org.pangrova.security.api.AuthorizationDeniedException;
import org.pangrova.security.api.event.SecurityPolicyChangeEvent;
import org.pangrova.security.api.event.SecurityPolicyEventPublisher;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import org.pangrova.security.api.policy.GrantStore;
import org.pangrova.security.api.policy.Privilege;
import org.pangrova.security.api.policy.PrivilegeGrant;
import org.pangrova.security.api.policy.Role;
import org.pangrova.security.api.policy.UserRoleAssignment;

/**
 * Tests the GRANT/REVOKE DDL execution path through {@link SecurityStatementHandler}.
 */
class SecurityStatementHandlerTest {

    private static final SecurityStatementHandler.GrantTarget ORDERS =
            new SecurityStatementHandler.GrantTarget("TABLE", "iceberg", "default", "orders");
    private static final SecurityStatementHandler.GrantTarget EVENTS =
            new SecurityStatementHandler.GrantTarget("TABLE", "iceberg", "default", "events");

    private MutableGrantStore grantStore;
    private SecurityPolicyEventPublisher eventPublisher;
    private List<SecurityPolicyChangeEvent> capturedEvents;
    private SecurityStatementHandler handler;

    @BeforeEach
    void setUp() {
        grantStore = new MutableGrantStore();
        eventPublisher = new SecurityPolicyEventPublisher();
        capturedEvents = new ArrayList<>();
        eventPublisher.subscribe(capturedEvents::add);
        handler = new SecurityStatementHandler(grantStore, eventPublisher);
    }

    @Test
    void grantAddsPrivilegeAndPublishesEvent() {
        handler.executeGrant("SELECT", ORDERS, "analyst", false, "admin");

        assertThat(grantStore.listGrants()).hasSize(1);
        PrivilegeGrant grant = grantStore.listGrants().get(0);
        assertThat(grant.roleName()).isEqualTo("analyst");
        assertThat(grant.privilege()).isEqualTo(Privilege.SELECT);
        assertThat(grant.catalog()).isEqualTo("iceberg");
        assertThat(grant.schema()).isEqualTo("default");
        assertThat(grant.table()).isEqualTo("orders");
        assertThat(grant.grantedBy()).isEqualTo("admin");

        assertThat(capturedEvents).hasSize(1);
        assertThat(capturedEvents.get(0))
                .isInstanceOf(SecurityPolicyChangeEvent.GrantChanged.class);
        SecurityPolicyChangeEvent.GrantChanged changed =
                (SecurityPolicyChangeEvent.GrantChanged) capturedEvents.get(0);
        assertThat(changed.roleName()).isEqualTo("analyst");
        assertThat(changed.privilege()).isEqualTo("SELECT");
    }

    @Test
    void revokeRemovesGrantAndPublishesEvent() {
        handler.executeGrant("INSERT", EVENTS, "writer", false, "admin");
        capturedEvents.clear();

        int revoked = handler.executeRevoke("INSERT", EVENTS, "writer", "admin");

        assertThat(revoked).isEqualTo(1);
        assertThat(grantStore.listGrants()).isEmpty();
        assertThat(capturedEvents).hasSize(1);
    }

    @Test
    void revokeNonExistentGrantReturnsZeroAndNoEvent() {
        int revoked = handler.executeRevoke("SELECT", ORDERS, "nobody", "admin");

        assertThat(revoked).isZero();
        assertThat(capturedEvents).isEmpty();
    }

    @Test
    void grantWithGrantOptionIsPreserved() {
        handler.executeGrant("SELECT", ORDERS, "lead_analyst", true, "admin");

        PrivilegeGrant grant = grantStore.listGrants().get(0);
        assertThat(grant.withGrantOption()).isTrue();
    }

    @Test
    void unknownPrivilegeThrowsIllegalArgument() {
        assertThatThrownBy(() ->
                handler.executeGrant("INVALID_PRIV", ORDERS, "analyst", false, "admin"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unknown privilege");
    }

    @Test
    void invalidRoleNameThrowsAuthorizationDenied() {
        assertThatThrownBy(() ->
                handler.executeGrant("SELECT", ORDERS, "role; DROP TABLE", false, "admin"))
                .isInstanceOf(AuthorizationDeniedException.class)
                .hasMessageContaining("Invalid SQL identifier");
    }

    @Test
    void grantThenRevokeLifecycleWithDynamicRbac() {
        // Wire up a DynamicRbacAccessControl sharing the same store and event bus
        var rlsFilter = new org.pangrova.security.core.rowfilter.RowLevelSecurityFilter(
                new org.pangrova.security.core.rowfilter.EmptyRowPolicyStore());
        var maskProcessor = new org.pangrova.security.core.mask.ColumnMaskingProcessor(
                new org.pangrova.security.core.mask.provider.empty.EmptyColumnMaskStore());
        var roles = new InMemoryRoleStore();
        roles.createRole(Role.of("analyst"));
        roles.grantRoleToUser(UserRoleAssignment.of("alice", "analyst"));
        var accessControl = new DynamicRbacAccessControl(
                grantStore, roles, rlsFilter, maskProcessor);

        // Initially denied
        var user = new org.pangrova.security.api.UserIdentity(
                "alice", java.util.Set.of("analyst"),
                java.util.Map.of(), java.time.Instant.now());
        assertThatThrownBy(() ->
                accessControl.checkCanSelectFromTable(user, "iceberg", "default", "orders"))
                .isInstanceOf(org.pangrova.security.api.AccessDeniedException.class);

        // GRANT via handler
        handler.executeGrant("SELECT", ORDERS, "analyst", false, "admin");

        // Event triggers cache refresh — now allowed
        org.assertj.core.api.Assertions.assertThatCode(() ->
                accessControl.checkCanSelectFromTable(user, "iceberg", "default", "orders"))
                .doesNotThrowAnyException();

        // REVOKE via handler
        handler.executeRevoke("SELECT", ORDERS, "analyst", "admin");

        // Event triggers cache refresh — denied again
        assertThatThrownBy(() ->
                accessControl.checkCanSelectFromTable(user, "iceberg", "default", "orders"))
                .isInstanceOf(org.pangrova.security.api.AccessDeniedException.class);
    }

    @Test
    void supportedPrivilegesReturnsAllEnumValues() {
        List<String> supported = handler.supportedPrivileges();
        assertThat(supported).contains("SELECT", "INSERT", "DELETE", "ALL", "OWNER");
        assertThat(supported).hasSize(Privilege.values().length);
    }

    /**
     * Minimal mutable in-process grant store for statement handler tests.
     */
    private static final class MutableGrantStore implements GrantStore {
        private final List<PrivilegeGrant> grants = new ArrayList<>();

        @Override
        public synchronized List<PrivilegeGrant> listGrants() {
            return List.copyOf(grants);
        }

        @Override
        public synchronized List<PrivilegeGrant> listGrantsForRole(String roleName) {
            return grants.stream().filter(g -> g.roleName().equals(roleName)).toList();
        }

        @Override
        public synchronized void addGrant(PrivilegeGrant grant) {
            grants.add(grant);
        }

        @Override
        public synchronized boolean revokeGrant(String grantId) {
            return grants.removeIf(g -> grantId != null && grantId.equals(g.id()));
        }

        @Override
        public synchronized int revokeGrants(String roleName, Privilege privilege,
                                             String catalog, String schema, String table) {
            int before = grants.size();
            grants.removeIf(g -> g.roleName().equals(roleName)
                    && g.privilege() == privilege
                    && (catalog == null || "*".equals(catalog) || catalog.equals(g.catalog()))
                    && (schema == null || "*".equals(schema) || schema.equals(g.schema()))
                    && (table == null || "*".equals(table) || table.equals(g.table())));
            return before - grants.size();
        }
    }
}

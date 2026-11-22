package org.pangrova.security.authorization;

import org.pangrova.testing.testkit.security.policy.InMemoryRoleStore;
import org.pangrova.testing.testkit.security.policy.StaticGrantStore;

import org.pangrova.security.policy.authorization.DynamicRbacAccessControl;

import org.pangrova.security.api.exception.AuthorizationDeniedException;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;
import org.pangrova.security.api.mask.ColumnMask;
import org.pangrova.security.mask.ColumnMaskingProcessor;
import org.pangrova.persistence.security.api.mask.EnrichedColumnMaskStore;
import org.pangrova.testing.testkit.security.mask.InMemoryEnrichedColumnMaskStore;
import org.pangrova.testing.testkit.security.rowfilter.EmptyRowPolicyStore;
import org.pangrova.security.rowfilter.RowLevelSecurityFilter;
import org.pangrova.security.api.identity.UserIdentity;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import org.pangrova.persistence.security.api.policy.GrantStore;
import org.pangrova.security.api.enums.Privilege;
import org.pangrova.security.api.policy.PrivilegeGrant;
import org.pangrova.security.api.policy.Role;
import org.pangrova.security.api.policy.UserRoleAssignment;

class MetadataReadAuthorityTest {
    private static final UserIdentity USER = new UserIdentity("reader", Set.of(), Map.of(), Instant.EPOCH);

    @Test
    void metadataRequiresFullSelectEvenWhenNoPolicyApplies() {
        var store = new InMemoryEnrichedColumnMaskStore();
        assertThatCode(() -> authority(store, null).checkCanReadTableMetadata(USER, "cat", "sch", "orders"))
                .doesNotThrowAnyException();
        assertThatThrownBy(() -> authority(store, "only_column").checkCanReadTableMetadata(USER, "cat", "sch", "orders"))
                .isInstanceOf(AuthorizationDeniedException.class);
        assertThatThrownBy(() -> authority(store, null).checkCanReadTableMetadata(USER, "other", "sch", "orders"))
                .isInstanceOf(AuthorizationDeniedException.class);
    }

    @Test
    void tableMaskCheckPreservesOrdinaryPriorityExemptionAndRoleSemantics() {
        var store = new InMemoryEnrichedColumnMaskStore();
        store.saveColumnMask(mask("disabled", "disabled", List.of(), List.of(), false));
        store.saveColumnMask(mask("other_role", "other", List.of("other"), List.of(), true));
        store.saveColumnMask(mask("first", "priority", List.of(), List.of("readers"), true));
        store.saveColumnMask(mask("later", "priority", List.of(), List.of(), true));
        var access = authority(store, null);
        assertThat(access.getColumnMasks(USER, "cat", "sch", "orders").mask("priority")).isEmpty();
        assertThatCode(() -> access.checkCanReadTableMetadata(USER, "cat", "sch", "orders"))
                .doesNotThrowAnyException();
        store.saveColumnMask(mask("historical", "retired_column", List.of(), List.of(), true));
        assertThatThrownBy(() -> access.checkCanReadTableMetadata(USER, "cat", "sch", "orders"))
                .isInstanceOf(AuthorizationDeniedException.class).hasMessageContaining("table metadata");
        store.deleteMask(store.getColumnMasks("cat", "sch", "orders", "retired_column").get(0).maskId());
        assertThatCode(() -> access.checkCanReadTableMetadata(USER, "cat", "sch", "orders"))
                .doesNotThrowAnyException();
    }

    @Test
    void unavailablePolicyAuthorityCannotBecomeAnUnfilteredRead() {
        var store = new EnrichedColumnMaskStore() {
            @Override
            public List<ColumnMask> getTableMasks(String catalog, String schema, String table) {
                throw new IllegalStateException("policy authority unavailable");
            }
            @Override
            public List<ColumnMask> getColumnMasks(String catalog, String schema, String table, String column) {
                throw new AssertionError("Metadata authority must inspect all table masks");
            }
            @Override public void saveColumnMask(ColumnMask mask) { throw new UnsupportedOperationException(); }
            @Override public void deleteMask(long id) { throw new UnsupportedOperationException(); }
        };
        assertThatThrownBy(() -> authority(store, null).checkCanReadTableMetadata(USER, "cat", "sch", "orders"))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("policy authority unavailable");
    }

    @Test
    void roleRevocationDuringGrantReadCannotEraseAnApplicableMask() {
        var roles = new InMemoryRoleStore();
        roles.createRole(Role.of("readers"));
        roles.grantRoleToUser(UserRoleAssignment.of(USER.principal(), "readers"));
        var grant = PrivilegeGrant.of("readers", Privilege.SELECT, "TABLE", "cat", "sch", "orders", null);
        var changingGrants = new GrantStore() {
            @Override
            public List<PrivilegeGrant> listGrants() {
                throw new AssertionError("Authorization must read only the effective role grants");
            }
            @Override
            public List<PrivilegeGrant> listGrantsForRoles(Set<String> roleNames) {
                roles.revokeRoleFromUser(USER.principal(), "readers");
                return roleNames.contains("readers") ? List.of(grant) : List.of();
            }
            @Override public void addGrant(PrivilegeGrant value) { throw new UnsupportedOperationException(); }
            @Override public boolean revokeGrant(String id) { throw new UnsupportedOperationException(); }
            @Override public int revokeGrants(String role, Privilege privilege, String catalog, String schema,
                    String table) { throw new UnsupportedOperationException(); }
        };
        var masks = new InMemoryEnrichedColumnMaskStore();
        masks.saveColumnMask(mask("secret", "secret", List.of("readers"), List.of(), true));
        var access = new DynamicRbacAccessControl(changingGrants, roles,
                new RowLevelSecurityFilter(new EmptyRowPolicyStore()), new ColumnMaskingProcessor(masks));
        assertThatThrownBy(() -> access.checkCanReadTableMetadata(USER, "cat", "sch", "orders"))
                .isInstanceOf(AuthorizationDeniedException.class).hasMessageContaining("table metadata");
    }

    private static ColumnMask mask(String name, String column, List<String> roles, List<String> exempt, boolean enabled) {
        return new ColumnMask(0, name, "cat", "sch", "orders", column, "NULL", roles, exempt, enabled, null);
    }

    private static DynamicRbacAccessControl authority(EnrichedColumnMaskStore masks, String column) {
        var roles = new InMemoryRoleStore();
        roles.createRole(Role.of("readers"));
        roles.grantRoleToUser(UserRoleAssignment.of(USER.principal(), "readers"));
        return new DynamicRbacAccessControl(new StaticGrantStore(List.of(
                PrivilegeGrant.of("readers", Privilege.SELECT, column == null ? "TABLE" : "COLUMN",
                        "cat", "sch", "orders", column))), roles,
                new RowLevelSecurityFilter(new EmptyRowPolicyStore()), new ColumnMaskingProcessor(masks));
    }
}

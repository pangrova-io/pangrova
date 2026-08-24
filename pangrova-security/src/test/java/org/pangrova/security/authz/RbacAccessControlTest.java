package org.pangrova.security.authz;

import org.pangrova.security.mask.ColumnMaskingProcessor;
import org.pangrova.security.mask.EmptyColumnMaskStore;
import org.pangrova.security.rowfilter.EmptyRowPolicyStore;
import org.pangrova.security.rowfilter.RowLevelSecurityFilter;
import org.pangrova.spi.security.AccessDeniedException;
import org.pangrova.spi.security.UserIdentity;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Validates: Requirements 18.2 — RBAC access control resolves privileges by role grants.
 */
class RbacAccessControlTest {

    private static final UserIdentity ANALYST = new UserIdentity(
            "alice", Set.of("analyst"), Map.of(), Instant.now());
    private static final UserIdentity ADMIN = new UserIdentity(
            "bob", Set.of("admin"), Map.of(), Instant.now());

    private final RowLevelSecurityFilter rlsFilter =
            new RowLevelSecurityFilter(new EmptyRowPolicyStore());
    private final ColumnMaskingProcessor maskProcessor =
            new ColumnMaskingProcessor(new EmptyColumnMaskStore());

    @Test
    void grantsSelectOnMatchingTable() {
        PrivilegeGrant grant = PrivilegeGrant.of(
                "analyst", Privilege.SELECT, "TABLE", "iceberg", "default", "orders", null);
        RbacAccessControl ac = new RbacAccessControl(List.of(grant), rlsFilter, maskProcessor);

        // Should not throw
        ac.checkCanSelectFromTable(ANALYST, "iceberg", "default", "orders");
    }

    @Test
    void deniesSelectOnMissingGrant() {
        RbacAccessControl ac = new RbacAccessControl(List.of(), rlsFilter, maskProcessor);

        assertThatThrownBy(() -> ac.checkCanSelectFromTable(ANALYST, "iceberg", "default", "orders"))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessageContaining("lacks SELECT");
    }

    @Test
    void wildcardGrantCoversAllTables() {
        PrivilegeGrant grant = PrivilegeGrant.of(
                "admin", Privilege.SELECT, "TABLE", "*", "*", "*", null);
        RbacAccessControl ac = new RbacAccessControl(List.of(grant), rlsFilter, maskProcessor);

        ac.checkCanSelectFromTable(ADMIN, "iceberg", "default", "orders");
        ac.checkCanSelectFromTable(ADMIN, "paimon", "analytics", "events");
    }

    @Test
    void checksInsertPrivilegeSeparately() {
        PrivilegeGrant selectGrant = PrivilegeGrant.of(
                "analyst", Privilege.SELECT, "TABLE", "iceberg", "default", "orders", null);
        RbacAccessControl ac = new RbacAccessControl(List.of(selectGrant), rlsFilter, maskProcessor);

        assertThatThrownBy(() -> ac.checkCanInsertIntoTable(ANALYST, "iceberg", "default", "orders"))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessageContaining("lacks INSERT");
    }

    @Test
    void checksColumnLevelSelect() {
        // Column grant with column=null acts as table-level in coversTable check
        // To test column-level denial, we need no table-level grant at all
        RbacAccessControl ac = new RbacAccessControl(List.of(), rlsFilter, maskProcessor);

        assertThatThrownBy(() ->
                ac.checkCanSelectFromColumns(ANALYST, "iceberg", "default", "orders", Set.of("secret")))
                .isInstanceOf(AccessDeniedException.class);
    }

    @Test
    void tableLevelSelectCoversAllColumns() {
        PrivilegeGrant tableGrant = PrivilegeGrant.of(
                "analyst", Privilege.SELECT, "TABLE", "iceberg", "default", "orders", null);
        RbacAccessControl ac = new RbacAccessControl(List.of(tableGrant), rlsFilter, maskProcessor);

        ac.checkCanSelectFromColumns(ANALYST, "iceberg", "default", "orders", Set.of("id", "amount", "secret"));
    }

    @Test
    void returnsEmptyRowFilterWhenNoPolicies() {
        RbacAccessControl ac = new RbacAccessControl(List.of(), rlsFilter, maskProcessor);
        Optional<String> filter = ac.getRowFilter(ANALYST, "iceberg", "default", "orders");
        assertThat(filter).isEmpty();
    }

    @Test
    void returnsEmptyColumnMaskWhenNoPolicies() {
        RbacAccessControl ac = new RbacAccessControl(List.of(), rlsFilter, maskProcessor);
        Optional<String> mask = ac.getColumnMask(ANALYST, "iceberg", "default", "orders", "email");
        assertThat(mask).isEmpty();
    }
}

package org.pangrova.coordinator.query.security;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.pangrova.security.api.AuthorizationDeniedException;
import org.pangrova.security.core.authz.DynamicRbacAccessControl;
import org.pangrova.security.api.policy.Privilege;
import org.pangrova.security.api.policy.PrivilegeGrant;
import org.pangrova.security.core.authz.StaticGrantStore;
import org.pangrova.security.api.mask.ColumnMask;
import org.pangrova.security.core.mask.ColumnMaskingProcessor;
import org.pangrova.security.core.mask.provider.memory.InMemoryEnrichedColumnMaskStore;
import org.pangrova.security.core.rowfilter.EmptyRowPolicyStore;
import org.pangrova.security.core.rowfilter.RowLevelSecurityFilter;
import org.pangrova.security.api.rowfilter.RowPolicy;
import org.pangrova.security.api.rowfilter.RowPolicyStore;
import org.pangrova.security.api.UserIdentity;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.pangrova.coordinator.testinfra.CoordinatorRoleFixtures.mappedRoles;

/** Metadata reads must not disclose raw counts, partitions or paths outside base-table policies. */
class MetadataReadPolicyTest {
    private static final UserIdentity USER = new UserIdentity("reader", Set.of("readers"), Map.of(), Instant.EPOCH);

    @ParameterizedTest
    @ValueSource(strings = {"snapshots", "history", "refs", "REFS", "files", "manifests", "entries",
            "tags", "options", "schemas", "branches", "ENTRIES", "partitions", "PARTITIONS"})
    void applicableBaseRowFilterRejectsMetadataReads(String suffix) {
        var rows = new RowPolicyStore() {
            @Override
            public List<RowPolicy> getPolicies(String catalog, String schema, String table) {
                return List.of(new RowPolicy(1, "tenant", catalog, schema, table,
                        "tenant_id = 1", List.of("readers"), true, 1, null));
            }
            @Override public void savePolicy(RowPolicy policy) { throw new UnsupportedOperationException(); }
            @Override public void deletePolicy(long id) { throw new UnsupportedOperationException(); }
        };
        rejectsMetadata(enforcer(rows, new InMemoryEnrichedColumnMaskStore()), suffix);
    }

    @ParameterizedTest
    @ValueSource(strings = {"snapshots", "history", "refs", "REFS", "files", "manifests", "entries",
            "tags", "options", "schemas", "branches", "ENTRIES", "partitions", "PARTITIONS"})
    void maskOnUnprojectedBaseColumnRejectsMetadataReads(String suffix) {
        var masks = new InMemoryEnrichedColumnMaskStore();
        masks.saveColumnMask(new ColumnMask(0, "redact", "cat", "sch", "orders", "secret",
                "NULL", List.of("readers"), List.of(), true, null));
        rejectsMetadata(enforcer(new EmptyRowPolicyStore(), masks), suffix);
    }

    private static void rejectsMetadata(AccessControlEnforcer enforcer, String suffix) {
        String table = "orders$" + suffix;
        assertThatCode(() -> enforcer.checkSelectFromTable(USER, "cat", "sch", "orders"))
                .doesNotThrowAnyException();
        assertThatThrownBy(() -> enforcer.checkTableAccess(List.of(List.of("cat", "sch", table)), USER))
                .isInstanceOf(AuthorizationDeniedException.class).hasMessageContaining("table metadata");
        assertThatThrownBy(() -> enforcer.checkSelectFromTable(USER, "cat", "sch", table))
                .isInstanceOf(AuthorizationDeniedException.class).hasMessageContaining("table metadata");
        assertThatThrownBy(() -> enforcer.checkColumnAccess("cat", "sch", table, Set.of("record_count"), USER))
                .isInstanceOf(AuthorizationDeniedException.class).hasMessageContaining("table metadata");
    }

    private static AccessControlEnforcer enforcer(RowPolicyStore rows, InMemoryEnrichedColumnMaskStore masks) {
        return new AccessControlEnforcer(new DynamicRbacAccessControl(
                new StaticGrantStore(List.of(PrivilegeGrant.of("readers", Privilege.SELECT,
                        "TABLE", "cat", "sch", "orders", null))), mappedRoles(Map.of("readers", "readers")),
                new RowLevelSecurityFilter(rows), new ColumnMaskingProcessor(masks)));
    }
}

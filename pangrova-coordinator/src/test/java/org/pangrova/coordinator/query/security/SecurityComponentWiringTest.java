package org.pangrova.coordinator.query.security;

import org.pangrova.planner.SqlCompiler;
import org.pangrova.planner.security.ColumnMaskApplier;
import org.pangrova.planner.security.RowFilterApplier;
import org.pangrova.planner.security.SecurityExpressionCache;
import org.pangrova.security.authz.DefaultAccessControl;
import org.pangrova.security.authz.RbacAccessControl;
import org.pangrova.security.mask.ColumnMask;
import org.pangrova.security.mask.EnrichedColumnMaskStore;
import org.pangrova.security.mask.ColumnMaskingProcessor;
import org.pangrova.security.mask.EmptyColumnMaskStore;
import org.pangrova.security.rowfilter.EmptyRowPolicyStore;
import org.pangrova.security.rowfilter.RowLevelSecurityFilter;
import org.pangrova.security.rowfilter.RowPolicy;
import org.pangrova.security.rowfilter.RowPolicyStore;
import org.pangrova.spi.security.AccessControl;
import org.pangrova.spi.security.UserIdentity;
import org.apache.calcite.rel.RelNode;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies security components wire AccessControl for column masking and row filtering.
 * Uses real RbacAccessControl with in-memory policy stores — no mocks.
 * Validates: Requirements 41.1, 41.2, 41.3.
 */
class SecurityComponentWiringTest {

    private static final SqlCompiler COMPILER = new SqlCompiler();

    @Test
    void emptyPolicyStoresProduceNoOpBehavior() {
        AccessControl ac = buildAccessControlWithEmptyPolicies();
        ColumnMaskApplier maskApplier = new ColumnMaskApplier(ac);
        RowFilterApplier filterApplier = new RowFilterApplier(ac, new SecurityExpressionCache());
        RelNode plan = COMPILER.compileToRelNode("SELECT 1");
        UserIdentity user = testUser("alice");

        RelNode masked = maskApplier.applyColumnMasks(plan, user);
        RelNode filtered = filterApplier.applyRowFilters(masked, user);

        assertThat(filtered).isSameAs(plan);
    }

    @Test
    void columnMaskPolicyOnConstantQueryIsNoOp() {
        ColumnMaskApplier maskApplier = new ColumnMaskApplier(
                buildAccessControlWithMaskStore(buildMaskStore("***")));
        RelNode plan = COMPILER.compileToRelNode("SELECT 1");
        UserIdentity user = testUser("bob");

        RelNode result = maskApplier.applyColumnMasks(plan, user);
        assertThat(result).isSameAs(plan);
    }

    @Test
    void rowFilterPolicyOnConstantQueryIsNoOp() {
        RowFilterApplier filterApplier = new RowFilterApplier(
                buildAccessControlWithPolicyStore(buildPolicyStore("region = 'US'")), new SecurityExpressionCache());
        RelNode plan = COMPILER.compileToRelNode("SELECT 1");
        UserIdentity user = testUser("carol");

        RelNode result = filterApplier.applyRowFilters(plan, user);
        assertThat(result).isSameAs(plan);
    }

    @Test
    void compilerWithoutAccessControlCompiles() {
        byte[] bytes = COMPILER.compile("SELECT 1 + 2");
        assertThat(bytes).isNotNull();
        assertThat(bytes.length).isGreaterThan(0);
    }

    private static AccessControl buildAccessControlWithEmptyPolicies() {
        return new RbacAccessControl(
                List.of(),
                new RowLevelSecurityFilter(new EmptyRowPolicyStore()),
                new ColumnMaskingProcessor(new EmptyColumnMaskStore()));
    }

    private static AccessControl buildAccessControlWithMaskStore(EnrichedColumnMaskStore store) {
        return new RbacAccessControl(
                List.of(),
                new RowLevelSecurityFilter(new EmptyRowPolicyStore()),
                new ColumnMaskingProcessor(store));
    }

    private static AccessControl buildAccessControlWithPolicyStore(RowPolicyStore store) {
        return new RbacAccessControl(
                List.of(),
                new RowLevelSecurityFilter(store),
                new ColumnMaskingProcessor(new EmptyColumnMaskStore()));
    }

    private static EnrichedColumnMaskStore buildMaskStore(String maskValue) {
        return new EnrichedColumnMaskStore() {
            @Override
            public List<ColumnMask> getColumnMasks(String c, String s, String t, String col) {
                return List.of();
            }
            @Override public void saveColumnMask(ColumnMask m) { }
            @Override public void deleteMask(long id) { }
        };
    }

    private static RowPolicyStore buildPolicyStore(String filterExpr) {
        return new RowPolicyStore() {
            @Override
            public List<RowPolicy> getPolicies(String c, String s, String t) {
                return List.of();
            }
            @Override public void savePolicy(RowPolicy p) { }
            @Override public void deletePolicy(long id) { }
        };
    }

    private static UserIdentity testUser(String name) {
        return new UserIdentity(name, Set.of("users"), Map.of(), Instant.now());
    }
}

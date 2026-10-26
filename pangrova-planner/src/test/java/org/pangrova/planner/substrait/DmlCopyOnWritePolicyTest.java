package org.pangrova.planner.substrait;

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexInterpreter;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;
import org.pangrova.planner.PangrovaPlanner;
import org.pangrova.planner.security.ColumnMaskApplier;
import org.pangrova.planner.security.RowFilterApplier;
import org.pangrova.security.authz.DynamicRbacAccessControl;
import org.pangrova.security.authz.InMemoryRoleStore;
import org.pangrova.security.authz.Role;
import org.pangrova.security.authz.StaticGrantStore;
import org.pangrova.security.authz.UserRoleAssignment;
import org.pangrova.security.mask.ColumnMask;
import org.pangrova.security.mask.ColumnMaskingProcessor;
import org.pangrova.security.mask.InMemoryEnrichedColumnMaskStore;
import org.pangrova.security.rowfilter.RowLevelSecurityFilter;
import org.pangrova.security.rowfilter.RowPolicy;
import org.pangrova.security.rowfilter.RowPolicyStore;
import org.pangrova.spi.security.UserIdentity;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests actual policy application and the relation consumed by the Native COW writer. */
class DmlCopyOnWritePolicyTest {
    private static final UserIdentity WRITER = new UserIdentity("writer", Set.of(), Map.of(), Instant.EPOCH);

    @Test
    void deletePredicatePreservesRowsOutsidePolicy() {
        LogicalFilter survivors = (LogicalFilter) lowered("DELETE FROM target_rows WHERE id > 1", true, false);

        assertThat(survivors.getInput()).isInstanceOf(TableScan.class);
        assertThat(value(survivors.getCondition(), survivors.getInput(), 1L, 1L, 10L)).isEqualTo(true);
        assertThat(value(survivors.getCondition(), survivors.getInput(), 2L, 2L, 20L)).isEqualTo(true);
        assertThat(value(survivors.getCondition(), survivors.getInput(), 3L, 1L, 30L)).isEqualTo(false);
    }

    @Test
    void deleteWithoutPredicateOnlyDeletesPolicyVisibleRows() {
        LogicalFilter survivors = (LogicalFilter) lowered("DELETE FROM target_rows", true, false);

        assertThat(survivors.getInput()).isInstanceOf(TableScan.class);
        assertThat(value(survivors.getCondition(), survivors.getInput(), 1L, 1L, 10L)).isEqualTo(false);
        assertThat(value(survivors.getCondition(), survivors.getInput(), 2L, 2L, 20L)).isEqualTo(true);
    }

    @Test
    void unknownPredicateAndUnknownRowPolicyRetainPhysicalRows() {
        LogicalFilter predicate = (LogicalFilter) lowered("DELETE FROM target_rows WHERE score > 0", false, false);
        LogicalFilter policy = (LogicalFilter) lowered("DELETE FROM target_rows", true, false);

        assertThat(value(predicate.getCondition(), predicate.getInput(), 1L, 1L, null)).isEqualTo(true);
        assertThat(value(policy.getCondition(), policy.getInput(), 2L, null, 20L)).isEqualTo(true);
    }

    @Test
    void updateUnknownPredicateRetainsOriginalNullValue() {
        LogicalProject result = (LogicalProject) lowered(
                "UPDATE target_rows SET score = 1 WHERE score > 0", false, false);

        assertThat(value(result.getProjects().get(2), result.getInput(), 1L, 1L, null))
                .isEqualTo(org.apache.calcite.rel.metadata.NullSentinel.INSTANCE);
    }

    @Test
    void unrestrictedDeleteWithoutPredicateProducesNoSurvivors() {
        LogicalFilter survivors = (LogicalFilter) lowered("DELETE FROM target_rows", false, false);

        assertThat(value(survivors.getCondition(), survivors.getInput(), 1L, 2L, 10L)).isEqualTo(false);
    }

    @Test
    void updatePreservesUnauthorizedRowsAndUnmaskedOriginalColumns() {
        LogicalProject result = (LogicalProject) lowered(
                "UPDATE target_rows SET score = score + 1 WHERE id > 1", true, true);

        assertThat(result.getInput()).isInstanceOf(TableScan.class);
        assertThat(value(result.getProjects().get(2), result.getInput(), 2L, 2L, 20L)).isEqualTo(20L);
        assertThat(value(result.getProjects().get(2), result.getInput(), 3L, 1L, 30L))
                .isEqualByComparingTo(java.math.BigDecimal.ONE);
    }

    @Test
    void deletePredicateReadsMaskedValuesWithoutOverwritingSurvivorValues() {
        LogicalFilter result = (LogicalFilter) lowered("DELETE FROM target_rows WHERE score > 0", false, true);

        assertThat(result.getInput()).isInstanceOf(TableScan.class);
        assertThat(value(result.getCondition(), result.getInput(), 1L, 1L, 30L)).isEqualTo(true);
    }

    @Test
    void maskedConjunctiveDeleteAndRowPolicyKeepCalciteFilterFlat() {
        LogicalFilter result = (LogicalFilter) lowered(
                "DELETE FROM target_rows WHERE score = 0 AND id > 0", true, true);

        assertThat(RexUtil.isFlat(result.getCondition())).isTrue();
    }

    @Test
    void updateAssignmentsFollowSqlOrderNotTableOrder() {
        LogicalProject result = (LogicalProject) lowered(
                "UPDATE target_rows SET score = 40, tenant = 7 WHERE id = 1", false, false);

        assertThat(value(result.getProjects().get(1), result.getInput(), 1L, 1L, 10L))
                .isEqualByComparingTo(java.math.BigDecimal.valueOf(7));
        assertThat(value(result.getProjects().get(2), result.getInput(), 1L, 1L, 10L))
                .isEqualByComparingTo(java.math.BigDecimal.valueOf(40));
    }

    @Test
    void maskedEqualityDoesNotSubstituteOriginalValuesWithPredicateConstants() {
        LogicalProject result = (LogicalProject) lowered(
                "UPDATE target_rows SET tenant = 7 WHERE score = 0", true, true);

        assertThat(result.getInput()).isInstanceOf(TableScan.class);
        assertThat(value(result.getProjects().get(2), result.getInput(), 1L, 1L, 30L)).isEqualTo(30L);
        assertThat(value(result.getProjects().get(2), result.getInput(), 2L, 2L, 20L)).isEqualTo(20L);
        assertThat(value(result.getProjects().get(1), result.getInput(), 1L, 1L, 30L))
                .isEqualByComparingTo(java.math.BigDecimal.valueOf(7));
        assertThat(value(result.getProjects().get(1), result.getInput(), 2L, 2L, 20L)).isEqualTo(2L);
    }

    @Test
    void mergePolicyRestrictsMatchesInsteadOfRemovingTargetRows() {
        LogicalTableModify modify = protectedPlan(
                "MERGE INTO target_rows t USING source_rows s ON t.id = s.id "
                        + "WHEN MATCHED THEN UPDATE SET score = s.score", true, true);
        LogicalJoin join = (LogicalJoin) ((LogicalProject) modify.getInput()).getInput();

        assertThat(join.getRight()).isInstanceOf(TableScan.class);
        assertThat(join.getCondition().getKind()).isEqualTo(SqlKind.AND);
        assertThat(join.getLeft().explain()).contains("LogicalProject");
        assertThat(new SubstraitPlanSerializer().serialize(new SubstraitConverter().convert(modify))).isNotEmpty();
    }

    @Test
    void maskedConjunctiveMergeAndRowPolicyKeepCalciteJoinFlat() {
        LogicalTableModify modify = protectedPlan(
                "MERGE INTO target_rows t USING source_rows s ON t.id = s.id "
                        + "WHEN MATCHED AND t.score = 0 AND t.id > 1 THEN UPDATE SET score = s.score",
                true, true);
        LogicalJoin join = (LogicalJoin) ((LogicalProject) modify.getInput()).getInput();

        assertThat(RexUtil.isFlat(join.getCondition())).isTrue();
        assertThat(join.getCondition().toString()).contains(">");
        assertThat(value(join.getCondition(), join, 1L, 1L, 99L, 1L, 1L, 11L))
                .isEqualTo(false);
        assertThat(value(join.getCondition(), join, 3L, 1L, 99L, 3L, 1L, 33L))
                .isEqualTo(true);
        assertThat(value(join.getCondition(), join, 3L, 1L, 99L, 3L, 2L, 33L))
                .isEqualTo(false);
    }

    @Test
    void conditionalMatchedUpdateWithInsertFailsBeforeLowering() {
        assertThatThrownBy(() -> protectedPlan(
                "MERGE INTO target_rows t USING source_rows s ON t.id = s.id "
                        + "WHEN MATCHED AND t.id > 1 THEN UPDATE SET score = s.score "
                        + "WHEN NOT MATCHED THEN INSERT (id, tenant, score) "
                        + "VALUES (s.id, s.tenant, s.score)", false, false))
                .hasMessageContaining("conditional UPDATE with INSERT");
    }

    private static RelNode lowered(String sql, boolean rowPolicy, boolean mask) {
        AtomicReference<RelNode> result = new AtomicReference<>();
        new DmlOperationConverter(node -> {
            result.set(node);
            return new SubstraitConverter().convert(node).getRoots().get(0).getInput();
        }, join -> { }).convert(protectedPlan(sql, rowPolicy, mask));
        return result.get();
    }

    private static LogicalTableModify protectedPlan(String sql, boolean rowPolicy, boolean mask) {
        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        schema.add("target_rows", table());
        schema.add("source_rows", table());
        RelNode plan = new PangrovaPlanner(schema).plan(sql).plan();
        DynamicRbacAccessControl authority = authority(rowPolicy, mask);
        plan = new ColumnMaskApplier(authority).applyColumnMasks(plan, WRITER);
        return (LogicalTableModify) new RowFilterApplier(authority).applyRowFilters(plan, WRITER);
    }

    private static DynamicRbacAccessControl authority(boolean rowPolicy, boolean mask) {
        InMemoryRoleStore roles = new InMemoryRoleStore();
        roles.createRole(Role.of("writers"));
        roles.grantRoleToUser(UserRoleAssignment.of("writer", "writers"));
        InMemoryEnrichedColumnMaskStore masks = new InMemoryEnrichedColumnMaskStore();
        if (mask) {
            for (String table : List.of("target_rows", "source_rows")) {
                masks.saveColumnMask(new ColumnMask(1, "hide_score", "default", "default", table,
                        "score", "0", List.of("writers"), List.of(), true, null));
            }
        }
        List<RowPolicy> policies = rowPolicy ? List.of(new RowPolicy(1, "tenant_rows", "default", "default",
                "target_rows", "tenant = 1", List.of("writers"), true, 1, null)) : List.of();
        return new DynamicRbacAccessControl(new StaticGrantStore(List.of()), roles,
                new RowLevelSecurityFilter(new ImmutablePolicies(policies)), new ColumnMaskingProcessor(masks));
    }

    private static AbstractTable table() {
        return new AbstractTable() {
            @Override
            public RelDataType getRowType(RelDataTypeFactory factory) {
                return factory.builder().add("id", SqlTypeName.BIGINT).nullable(true)
                        .add("tenant", SqlTypeName.BIGINT).nullable(true)
                        .add("score", SqlTypeName.BIGINT).nullable(true).build();
            }
        };
    }

    @SuppressWarnings("rawtypes")
    private static Comparable value(RexNode expression, RelNode input, Long... row) {
        Map<RexNode, Comparable> bindings = new HashMap<>();
        for (int i = 0; i < row.length; i++) {
            bindings.put(new RexInputRef(i, input.getRowType().getFieldList().get(i).getType()),
                    row[i] == null ? org.apache.calcite.rel.metadata.NullSentinel.INSTANCE : row[i]);
        }
        return RexInterpreter.evaluate(expression, bindings);
    }

    private record ImmutablePolicies(List<RowPolicy> policies) implements RowPolicyStore {
        @Override
        public List<RowPolicy> getPolicies(String catalog, String schema, String table) {
            return policies.stream().filter(policy -> policy.catalogName().equals(catalog)
                    && policy.schemaName().equals(schema) && policy.tableName().equals(table)).toList();
        }

        @Override
        public void savePolicy(RowPolicy policy) {
            throw new UnsupportedOperationException("Immutable test policy catalog");
        }

        @Override
        public void deletePolicy(long id) {
            throw new UnsupportedOperationException("Immutable test policy catalog");
        }
    }
}

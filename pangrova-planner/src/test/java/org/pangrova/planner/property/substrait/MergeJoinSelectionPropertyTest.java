package org.pangrova.planner.property.substrait;

import com.google.common.collect.ImmutableList;
import io.substrait.expression.Expression;
import io.substrait.relation.Rel;
import io.substrait.relation.Sort;
import io.substrait.relation.physical.MergeJoin;
import net.jqwik.api.*;
import org.pangrova.planner.cost.join.PangrovaSortMergeJoin;
import org.pangrova.planner.cost.join.SortMergeJoinConfig;
import org.pangrova.planner.substrait.FunctionMapper;
import org.pangrova.planner.substrait.PlanNodeConverter;
import org.pangrova.planner.substrait.TypeMapper;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property 6: Planner MergeJoin Selection.
 *
 * <p><b>Validates: Requirements 2.4</b></p>
 *
 * <p>For any equi-join where both inputs have a physical sort order matching
 * the join keys, the Java planner SHALL emit a MergeJoinRel (via
 * PangrovaSortMergeJoin → standard MergeJoinRel) instead of a HashJoinRel.</p>
 *
 * <p>This test generates random join plans with varying input sort orders and
 * verifies that PlanNodeConverter always makes the standard SortRel precondition
 * explicit when processing PangrovaSortMergeJoin nodes.</p>
 */
@Tag("Feature: native-velox-feature-completeness")
@Tag("Property 6: Planner MergeJoin Selection")
class MergeJoinSelectionPropertyTest {

    private static final RelDataTypeFactory TYPE_FACTORY =
            new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);

    /**
     * When PlanNodeConverter receives an PangrovaSortMergeJoin with both inputs
     * sorted on join keys, it preserves that standard SortRel. When either input
     * is unsorted, it inserts the missing standard SortRel before MergeJoinRel.
     */
    @Property(tries = 100)
    void mergeJoinEmittedWithCorrectPreSortedFlag(
            @ForAll("joinConfigurations") JoinTestConfig config) {

        RelOptCluster cluster = buildCluster();
        RexBuilder rexBuilder = cluster.getRexBuilder();

        // Build left and right inputs with appropriate collation traits
        RelNode leftInput = buildInput(cluster, config.leftColumnCount(),
                config.leftSortedOnKeys() ? config.leftJoinKeyIndices() : List.of());
        RelNode rightInput = buildInput(cluster, config.rightColumnCount(),
                config.rightSortedOnKeys() ? config.rightJoinKeyIndices() : List.of());

        // Build equi-join condition: left.key[i] = right.key[i]
        RexNode condition = buildEquiJoinCondition(rexBuilder, leftInput, rightInput,
                config.leftJoinKeyIndices(), config.rightJoinKeyIndices());

        // Create PangrovaSortMergeJoin (what the CBO would produce)
        PangrovaSortMergeJoin mergeJoin = new PangrovaSortMergeJoin(new SortMergeJoinConfig(
                cluster,
                leftInput.getTraitSet(),
                leftInput,
                rightInput,
                condition,
                Set.of(),
                config.joinType()));

        // Convert via PlanNodeConverter
        PlanNodeConverter converter = new PlanNodeConverter(
                new TypeMapper(), new FunctionMapper(), rexBuilder);
        Rel result = converter.visit(mergeJoin);

        // Verify: result is the official physical MergeJoinRel.
        assertThat(result)
                .as("PangrovaSortMergeJoin must produce standard MergeJoinRel")
                .isInstanceOf(MergeJoin.class);

        MergeJoin standardJoin = (MergeJoin) result;

        assertThat(standardJoin.getLeft())
                .as("MergeJoin left input must make sorting explicit")
                .isInstanceOf(Sort.class);
        assertThat(standardJoin.getRight())
                .as("MergeJoin right input must make sorting explicit")
                .isInstanceOf(Sort.class);

        for (Rel input : standardJoin.getInputs()) {
            Sort sort = (Sort) input;
            assertThat(sort.getSortFields()).extracting(Expression.SortField::direction)
                    .containsOnly(Expression.SortDirection.ASC_NULLS_FIRST);
        }

        // Verify: join type is correctly mapped
        MergeJoin.JoinType expectedJoinType = mapExpectedJoinType(config.joinType());
        assertThat(standardJoin.getJoinType())
                .as("Join type must be correctly mapped")
                .isEqualTo(expectedJoinType);

        // Verify: key indices are correct
        assertThat(standardJoin.getKeys().stream()
                .map(key -> keyOffset(key.getLeft())).toList())
                .as("Left join keys must match")
                .isEqualTo(config.leftJoinKeyIndices());
        assertThat(standardJoin.getKeys().stream()
                .map(key -> keyOffset(key.getRight())).toList())
                .as("Right join keys must match")
                .isEqualTo(config.rightJoinKeyIndices());

        assertThat(standardJoin.getInputs())
                .as("MergeJoinRel must have exactly two inputs")
                .hasSize(2);
    }

    // ── Providers ──

    @Provide
    Arbitrary<JoinTestConfig> joinConfigurations() {
        Arbitrary<JoinRelType> joinTypes = Arbitraries.of(
                JoinRelType.INNER, JoinRelType.LEFT, JoinRelType.RIGHT, JoinRelType.FULL);

        Arbitrary<Integer> keyCount = Arbitraries.integers().between(1, 3);
        Arbitrary<Integer> columnCount = Arbitraries.integers().between(2, 5);
        Arbitrary<Boolean> sorted = Arbitraries.of(true, false);

        return Combinators.combine(joinTypes, keyCount, columnCount, columnCount, sorted, sorted)
                .as((joinType, keys, leftCols, rightCols, leftSorted, rightSorted) -> {
                    int effectiveLeftCols = Math.max(leftCols, keys);
                    int effectiveRightCols = Math.max(rightCols, keys);

                    List<Integer> leftKeys = buildKeyIndices(keys);
                    List<Integer> rightKeys = buildKeyIndices(keys);

                    return new JoinTestConfig(
                            joinType, effectiveLeftCols, effectiveRightCols,
                            leftKeys, rightKeys, leftSorted, rightSorted);
                });
    }

    // ── Helper methods ──

    private static List<Integer> buildKeyIndices(int keyCount) {
        return java.util.stream.IntStream.range(0, keyCount).boxed().toList();
    }

    private static RelOptCluster buildCluster() {
        VolcanoPlanner planner = new VolcanoPlanner();
        planner.addRelTraitDef(org.apache.calcite.plan.ConventionTraitDef.INSTANCE);
        return RelOptCluster.create(planner, new RexBuilder(TYPE_FACTORY));
    }

    /**
     * Builds a RelNode input with the specified column count and optional sort order.
     * Uses LogicalValues with a single row as the base, wrapped in LogicalSort when
     * sort keys are specified to carry collation in the trait set.
     */
    private static RelNode buildInput(RelOptCluster cluster, int columnCount,
                                      List<Integer> sortKeyIndices) {
        RelDataTypeFactory.Builder typeBuilder = TYPE_FACTORY.builder();
        for (int i = 0; i < columnCount; i++) {
            typeBuilder.add("COL_" + i, SqlTypeName.INTEGER);
        }
        RelDataType rowType = typeBuilder.build();

        // Create LogicalValues with one row (Substrait VirtualTableScan requires ≥1 row)
        RexBuilder rexBuilder = cluster.getRexBuilder();
        ImmutableList.Builder<ImmutableList<org.apache.calcite.rex.RexLiteral>> tuples =
                ImmutableList.builder();
        ImmutableList.Builder<org.apache.calcite.rex.RexLiteral> row = ImmutableList.builder();
        for (int i = 0; i < columnCount; i++) {
            row.add(rexBuilder.makeExactLiteral(java.math.BigDecimal.valueOf(i + 1)));
        }
        tuples.add(row.build());
        LogicalValues values = LogicalValues.create(cluster, rowType, tuples.build());

        if (sortKeyIndices.isEmpty()) {
            return values;
        }

        // Create LogicalSort with collation — the sort node carries collation
        // in its trait set via traitSet.plus(collation)
        List<RelFieldCollation> fieldCollations = sortKeyIndices.stream()
                .map(idx -> new RelFieldCollation(idx, RelFieldCollation.Direction.ASCENDING))
                .toList();
        RelCollation collation = RelCollations.of(fieldCollations);

        // LogicalSort.create sets the collation in the trait set
        LogicalSort sort = LogicalSort.create(values, collation, null, null);

        // Ensure the trait set carries the collation (required by hasMatchingCollation)
        RelTraitSet traitSetWithCollation = sort.getTraitSet().plus(collation);
        return sort.copy(traitSetWithCollation, sort.getInputs());
    }

    /**
     * Builds an equi-join condition: left.key[0] = right.key[0] AND ...
     */
    private static RexNode buildEquiJoinCondition(RexBuilder rexBuilder,
                                                   RelNode left, RelNode right,
                                                   List<Integer> leftKeys,
                                                   List<Integer> rightKeys) {
        int leftFieldCount = left.getRowType().getFieldCount();
        RexNode condition = null;

        for (int i = 0; i < leftKeys.size(); i++) {
            RexNode leftRef = rexBuilder.makeInputRef(
                    left.getRowType().getFieldList().get(leftKeys.get(i)).getType(),
                    leftKeys.get(i));
            RexNode rightRef = rexBuilder.makeInputRef(
                    right.getRowType().getFieldList().get(rightKeys.get(i)).getType(),
                    leftFieldCount + rightKeys.get(i));
            RexNode eq = rexBuilder.makeCall(
                    org.apache.calcite.sql.fun.SqlStdOperatorTable.EQUALS,
                    leftRef, rightRef);

            condition = (condition == null) ? eq
                    : rexBuilder.makeCall(
                    org.apache.calcite.sql.fun.SqlStdOperatorTable.AND, condition, eq);
        }
        return condition;
    }

    private static int keyOffset(io.substrait.expression.FieldReference reference) {
        return ((io.substrait.expression.FieldReference.StructField)
                reference.segments().get(0)).offset();
    }

    private static MergeJoin.JoinType mapExpectedJoinType(JoinRelType joinType) {
        return switch (joinType) {
            case INNER -> MergeJoin.JoinType.INNER;
            case LEFT -> MergeJoin.JoinType.LEFT;
            case RIGHT -> MergeJoin.JoinType.RIGHT;
            case FULL -> MergeJoin.JoinType.OUTER;
            default -> throw new IllegalArgumentException("Unsupported: " + joinType);
        };
    }

    // ── Test configuration record ──

    record JoinTestConfig(
            JoinRelType joinType,
            int leftColumnCount,
            int rightColumnCount,
            List<Integer> leftJoinKeyIndices,
            List<Integer> rightJoinKeyIndices,
            boolean leftSortedOnKeys,
            boolean rightSortedOnKeys) {

        @Override
        public String toString() {
            return String.format("Join(%s, leftCols=%d, rightCols=%d, keys=%d, leftSorted=%s, rightSorted=%s)",
                    joinType, leftColumnCount, rightColumnCount,
                    leftJoinKeyIndices.size(), leftSortedOnKeys, rightSortedOnKeys);
        }
    }
}

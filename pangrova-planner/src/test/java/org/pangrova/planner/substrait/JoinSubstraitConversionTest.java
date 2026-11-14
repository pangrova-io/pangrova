package org.pangrova.planner.substrait;

import org.pangrova.planner.enums.UnsupportedNodeKind;

import com.google.common.collect.ImmutableList;
import io.substrait.plan.Plan;
import io.substrait.plan.ImmutablePlan;
import io.substrait.plan.ImmutableRoot;
import io.substrait.plan.ImmutableExecutionBehavior;
import io.substrait.plan.PlanProtoConverter;
import io.substrait.relation.Rel;
import io.substrait.relation.Join;
import io.substrait.relation.Sort;
import io.substrait.relation.physical.BroadcastExchange;
import io.substrait.relation.physical.HashJoin;
import io.substrait.relation.physical.MergeJoin;
import io.substrait.relation.physical.NestedLoopJoin;
import org.pangrova.planner.cost.join.PangrovaBroadcastJoin;
import org.pangrova.planner.cost.join.PangrovaHashJoin;
import org.pangrova.planner.cost.join.PangrovaNestedLoopJoin;
import org.pangrova.planner.cost.join.PangrovaSortMergeJoin;
import org.pangrova.planner.cost.join.BroadcastJoinConfig;
import org.pangrova.planner.cost.join.HashJoinConfig;
import org.pangrova.planner.cost.join.NestedLoopJoinConfig;
import org.pangrova.planner.cost.join.SortMergeJoinConfig;
import org.pangrova.planner.cost.distribution.PangrovaDistribution;
import org.pangrova.planner.cost.distribution.PangrovaExchange;
import org.pangrova.planner.validation.NativeCapabilityRegistry;
import org.pangrova.planner.validation.SubstraitPlanValidator;
import org.pangrova.planner.enums.ValidationStatus;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.math.BigDecimal;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Verifies that all physical join operators (HashJoin, SortMergeJoin,
 * NestedLoopJoin, BroadcastJoin) convert to correct Substrait JoinRel
 * fields including join type, keys, residual predicates, and distribution.
 */
class JoinSubstraitConversionTest {

    private static final RelDataTypeFactory TYPE_FACTORY =
            new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);

    private RelOptCluster cluster;
    private RexBuilder rexBuilder;
    private PlanNodeConverter converter;

    @BeforeEach
    void setUp() {
        VolcanoPlanner planner = new VolcanoPlanner();
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        cluster = RelOptCluster.create(planner, new RexBuilder(TYPE_FACTORY));
        rexBuilder = cluster.getRexBuilder();
        converter = new PlanNodeConverter(new TypeMapper(), new FunctionMapper(), rexBuilder);
    }

    @ParameterizedTest
    @EnumSource(value = JoinRelType.class,
            names = {"ASOF", "LEFT_ASOF", "LEFT_MARK"},
            mode = EnumSource.Mode.EXCLUDE)
    void hashJoinConvertsAllJoinTypes(JoinRelType joinType) {
        PangrovaHashJoin hashJoin = buildHashJoin(joinType);

        Rel result = converter.visit(hashJoin);

        assertThat(result).isInstanceOf(HashJoin.class);
        HashJoin join = (HashJoin) result;
        assertThat(join.getJoinType()).isEqualTo(expectedHashJoinType(joinType));
        assertThat(join.getKeys()).hasSize(1);
        assertThat(keyOffset(join.getKeys().get(0).getLeft())).isZero();
        assertThat(keyOffset(join.getKeys().get(0).getRight())).isZero();
        assertCanonicalJoinSerialization(hashJoin, "hash_join");
    }

    @ParameterizedTest
    @EnumSource(value = JoinRelType.class,
            names = {"ASOF", "LEFT_ASOF", "LEFT_MARK"},
            mode = EnumSource.Mode.EXCLUDE)
    void broadcastJoinConvertsAllJoinTypes(JoinRelType joinType) {
        PangrovaBroadcastJoin broadcastJoin = buildBroadcastJoin(joinType);

        Rel result = converter.visit(broadcastJoin);

        assertThat(result).isInstanceOf(HashJoin.class);
        HashJoin join = (HashJoin) result;
        assertThat(join.getJoinType()).isEqualTo(expectedHashJoinType(joinType));
        assertThat(join.getRight()).isInstanceOf(BroadcastExchange.class);
        assertThat(((BroadcastExchange) join.getRight()).getPartitionCount())
                .isEqualTo(PhysicalJoinConverter.UNBOUND_PARTITION_COUNT);
        assertCanonicalJoinSerialization(broadcastJoin, "hash_join");
    }

    @ParameterizedTest
    @EnumSource(value = JoinRelType.class,
            names = {"ASOF", "LEFT_ASOF", "LEFT_MARK"},
            mode = EnumSource.Mode.EXCLUDE)
    void sortMergeJoinConvertsAllJoinTypes(JoinRelType joinType) {
        PangrovaSortMergeJoin mergeJoin = buildSortMergeJoin(joinType);

        Rel result = converter.visit(mergeJoin);

        assertThat(result).isInstanceOf(MergeJoin.class);
        MergeJoin join = (MergeJoin) result;
        assertThat(join.getJoinType()).isEqualTo(expectedMergeJoinType(joinType));
        assertThat(join.getKeys()).hasSize(1);
        assertThat(join.getLeft()).isInstanceOf(Sort.class);
        assertThat(join.getRight()).isInstanceOf(Sort.class);
        assertNativeValidation(mergeJoin);
        assertCanonicalJoinSerialization(mergeJoin, "merge_join");
    }

    @Test
    void sortMergeJoinPreservesAsymmetricInputSchema() throws Exception {
        RelNode left = buildInput(1);
        RelNode right = buildInput(3);
        RexNode condition = buildEquiCondition(left, right, 0, 0);
        PangrovaSortMergeJoin mergeJoin = new PangrovaSortMergeJoin(
                new SortMergeJoinConfig(
                        cluster, left.getTraitSet(), left, right,
                        condition, Set.of(), JoinRelType.INNER));

        MergeJoin result = (MergeJoin) converter.visit(mergeJoin);

        assertThat(result.getLeft().getRecordType().fields()).hasSize(1);
        assertThat(result.getRight().getRecordType().fields()).hasSize(3);
        assertThat(result.getRecordType().fields()).hasSize(4);

        var plan = ImmutablePlan.builder()
                .addRoots(ImmutableRoot.builder().input(result)
                        .addAllNames(List.of("left_key", "right_key", "right_value", "right_extra"))
                        .build())
                .executionBehavior(ImmutableExecutionBehavior.builder()
                        .variableEvaluationMode(Plan.ExecutionBehavior.VariableEvaluationMode.PER_PLAN)
                        .build())
                .build();
        var wire = io.substrait.proto.Plan.parseFrom(new SubstraitPlanSerializer().serialize(plan));
        var join = wire.getRelations(0).getRoot().getInput().getMergeJoin();
        assertThat(join.getKeysCount()).isPositive();
        assertThat(join.getLeftKeysCount()).isZero();
        assertThat(join.getRightKeysCount()).isZero();

        var sdkWire = new PlanProtoConverter(new PangrovaExtensionProtoConverter()).toProto(plan);
        var legacyJoin = sdkWire.getRelations(0).getRoot().getInput().getMergeJoin();
        assertThat(legacyJoin.getLeftKeysCount()).isPositive();
        var conflicting = sdkWire.toBuilder();
        conflicting.getRelationsBuilder(0).getRootBuilder().getInputBuilder()
                .getMergeJoinBuilder().setLeftKeys(0,
                        io.substrait.proto.Expression.FieldReference.getDefaultInstance());
        assertThat(conflicting.getRelations(0).getRoot().getInput().getMergeJoin().getLeftKeys(0))
                .isNotEqualTo(legacyJoin.getLeftKeys(0));
        assertThatThrownBy(() -> SubstraitProtoNormalizer.normalize(conflicting.build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("differ from canonical keys");
    }

    @ParameterizedTest
    @EnumSource(value = JoinRelType.class,
            names = {"ASOF", "LEFT_ASOF", "LEFT_MARK"},
            mode = EnumSource.Mode.EXCLUDE)
    void nestedLoopJoinConvertsAllJoinTypes(JoinRelType joinType) {
        PangrovaNestedLoopJoin nestedLoopJoin = buildNestedLoopJoin(joinType);

        Rel result = converter.visit(nestedLoopJoin);

        assertThat(result).isInstanceOf(NestedLoopJoin.class);
        NestedLoopJoin join = (NestedLoopJoin) result;
        assertThat(join.getJoinType()).isEqualTo(expectedNestedLoopJoinType(joinType));
        assertThat(join.getCondition()).isNotNull();
        assertNativeValidation(nestedLoopJoin);
    }

    @Test
    void mergeJoinRequiresItsCanonicalCapability() {
        NativeCapabilityRegistry registry = NativeCapabilityRegistry.defaults();
        registry.update(Set.of("ReadRel", "SortRel"), Set.of("FieldReference"), Set.of());
        Plan plan = new SubstraitConverter().convert(buildSortMergeJoin(JoinRelType.INNER));

        var result = new SubstraitPlanValidator(registry).validate(plan);

        assertThat(result.unsupportedNodes()).singleElement().satisfies(node -> {
            assertThat(node.nodeType()).isEqualTo("MergeJoinRel");
            assertThat(node.kind()).isEqualTo(UnsupportedNodeKind.RELATION);
        });
    }

    @Test
    void mergeJoinKeysAreValidatedAtJoinDepth() {
        NativeCapabilityRegistry registry = NativeCapabilityRegistry.defaults();
        registry.update(Set.of("ReadRel", "SortRel", "MergeJoinRel"), Set.of(), Set.of());
        Plan plan = new SubstraitConverter().convert(buildSortMergeJoin(JoinRelType.INNER));

        var result = new SubstraitPlanValidator(registry).validate(plan);

        assertThat(result.unsupportedNodes()).filteredOn(node -> node.depth() == 0)
                .hasSize(2).allSatisfy(node -> {
                    assertThat(node.nodeType()).isEqualTo("FieldReference");
                    assertThat(node.kind()).isEqualTo(UnsupportedNodeKind.EXPRESSION);
                });
    }

    @Test
    void nestedLoopJoinConditionCannotBypassFunctionValidation() {
        NativeCapabilityRegistry registry = NativeCapabilityRegistry.defaults();
        registry.update(Set.of("ReadRel", "NestedLoopJoinRel"),
                Set.of("FieldReference", "ScalarFunction"), Set.of());
        Plan plan = new SubstraitConverter().convert(buildNestedLoopJoin(JoinRelType.INNER));

        var result = new SubstraitPlanValidator(registry).validate(plan);

        assertThat(result.unsupportedNodes()).singleElement().satisfies(node -> {
            assertThat(node.nodeType()).isEqualTo("equal");
            assertThat(node.kind()).isEqualTo(UnsupportedNodeKind.FUNCTION);
        });
    }

    @Test
    void mergeJoinResidualCannotBypassFunctionValidation() {
        RelNode left = buildInput(3);
        RelNode right = buildInput(3);
        RexNode residual = rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN,
                rexBuilder.makeInputRef(left.getRowType().getFieldList().get(1).getType(), 1),
                rexBuilder.makeInputRef(right.getRowType().getFieldList().get(1).getType(), 4));
        RexNode condition = rexBuilder.makeCall(SqlStdOperatorTable.AND,
                buildEquiCondition(left, right, 0, 0), residual);
        var join = new PangrovaSortMergeJoin(new SortMergeJoinConfig(
                cluster, left.getTraitSet(), left, right, condition, Set.of(), JoinRelType.INNER));
        NativeCapabilityRegistry registry = NativeCapabilityRegistry.defaults();
        registry.update(Set.of("ReadRel", "SortRel", "MergeJoinRel", "JoinRel"),
                Set.of("FieldReference", "ScalarFunction"), Set.of("equal", "and"));
        Plan plan = new SubstraitConverter().convert(join);

        var result = new SubstraitPlanValidator(registry).validate(plan);

        assertThat(result.unsupportedNodes()).singleElement().satisfies(node -> {
            assertThat(node.nodeType()).isEqualTo("gt");
            assertThat(node.kind()).isEqualTo(UnsupportedNodeKind.FUNCTION);
        });
    }

    private static void assertNativeValidation(RelNode join) {
        Plan plan = new SubstraitConverter().convert(join);
        var result = new SubstraitPlanValidator(NativeCapabilityRegistry.defaults()).validate(plan);
        assertThat(result.status()).isEqualTo(ValidationStatus.FULLY_NATIVE);
        assertThat(result.unsupportedNodes()).isEmpty();
    }

    private static void assertCanonicalJoinSerialization(RelNode join, String relationField) {
        Plan plan = new SubstraitConverter().convert(join);
        try {
            var wire = io.substrait.proto.Plan.parseFrom(new SubstraitPlanSerializer().serialize(plan));
            var relation = wire.getRelations(0).getRoot().getInput();
            var field = relation.getDescriptorForType().findFieldByName(relationField);
            assertThat(relation.hasField(field)).isTrue();
            var physicalJoin = (com.google.protobuf.Message) relation.getField(field);
            var descriptor = physicalJoin.getDescriptorForType();
            assertThat(physicalJoin.getRepeatedFieldCount(descriptor.findFieldByName("keys"))).isOne();
            assertThat(physicalJoin.getRepeatedFieldCount(descriptor.findFieldByName("left_keys"))).isZero();
            assertThat(physicalJoin.getRepeatedFieldCount(descriptor.findFieldByName("right_keys"))).isZero();
        } catch (com.google.protobuf.InvalidProtocolBufferException failure) {
            throw new AssertionError("Canonical Join plan cannot reparse", failure);
        }
    }

    @Test
    void hashJoinFullPlanConversionProducesValidPlan() {
        PangrovaHashJoin hashJoin = buildHashJoin(JoinRelType.INNER);
        SubstraitConverter substraitConverter = new SubstraitConverter();

        Plan plan = substraitConverter.convert(hashJoin);

        assertThat(plan.getRoots()).hasSize(1);
        assertThat(plan.getRoots().get(0).getInput()).isNotNull();
        assertThat(plan.getRoots().get(0).getNames()).isNotEmpty();
    }

    @Test
    void nestedLoopJoinFullPlanConversionProducesValidPlan() {
        PangrovaNestedLoopJoin nestedLoopJoin = buildNestedLoopJoin(JoinRelType.INNER);
        SubstraitConverter substraitConverter = new SubstraitConverter();

        Plan plan = substraitConverter.convert(nestedLoopJoin);

        assertThat(plan.getRoots()).hasSize(1);
        assertThat(plan.getRoots().get(0).getInput()).isNotNull();
        assertThat(plan.getRoots().get(0).getNames()).isNotEmpty();
    }

    @Test
    void hashJoinSemiTypeProducesCorrectDetail() {
        PangrovaHashJoin hashJoin = buildHashJoin(JoinRelType.SEMI);
        SubstraitConverter substraitConverter = new SubstraitConverter();

        Plan plan = substraitConverter.convert(hashJoin);

        Rel rootInput = plan.getRoots().get(0).getInput();
        assertThat(rootInput).isInstanceOf(HashJoin.class);
        assertThat(((HashJoin) rootInput).getJoinType())
                .isEqualTo(HashJoin.JoinType.LEFT_SEMI);
    }

    @Test
    void hashJoinAntiTypeProducesCorrectDetail() {
        PangrovaHashJoin hashJoin = buildHashJoin(JoinRelType.ANTI);
        SubstraitConverter substraitConverter = new SubstraitConverter();

        Plan plan = substraitConverter.convert(hashJoin);

        Rel rootInput = plan.getRoots().get(0).getInput();
        assertThat(rootInput).isInstanceOf(HashJoin.class);
        assertThat(((HashJoin) rootInput).getJoinType())
                .isEqualTo(HashJoin.JoinType.LEFT_ANTI);
    }

    @Test
    void hashJoinMultipleKeysProducesCorrectFieldReferences() {
        RelNode left = buildInput(4);
        RelNode right = buildInput(4);
        RexNode condition = buildMultiKeyCondition(left, right);
        PangrovaHashJoin hashJoin = new PangrovaHashJoin(new HashJoinConfig(
                cluster, left.getTraitSet(), left, right,
                condition, Set.of(), JoinRelType.INNER));

        Rel result = converter.visit(hashJoin);

        HashJoin join = (HashJoin) result;
        assertThat(join.getKeys()).hasSize(2);
        assertThat(join.getKeys()).extracting(k -> keyOffset(k.getLeft()))
                .containsExactly(0, 1);
        assertThat(join.getKeys()).extracting(k -> keyOffset(k.getRight()))
                .containsExactly(0, 1);
    }

    @Test
    void outerJoinResidualStaysInsideJoinCondition() {
        RelNode left = buildInput(2);
        RelNode right = buildInput(2);
        RexNode equality = buildEquiCondition(left, right, 0, 0);
        RexNode leftValue = rexBuilder.makeInputRef(
                left.getRowType().getFieldList().get(1).getType(), 1);
        RexNode rightValue = rexBuilder.makeInputRef(
                right.getRowType().getFieldList().get(1).getType(), 3);
        RexNode residual = rexBuilder.makeCall(SqlStdOperatorTable.NOT_EQUALS,
                leftValue, rightValue);
        RexNode condition = rexBuilder.makeCall(SqlStdOperatorTable.AND,
                equality, residual);
        PangrovaHashJoin physical = new PangrovaHashJoin(new HashJoinConfig(
                cluster, left.getTraitSet(), left, right,
                condition, Set.of(), JoinRelType.LEFT));

        Rel result = converter.visit(physical);

        assertThat(result).isInstanceOf(Join.class);
        assertThat(((Join) result).getJoinType()).isEqualTo(Join.JoinType.LEFT);
        assertThat(((Join) result).getCondition()).isNotNull();
        assertThat(((Join) result).getPostJoinFilter()).isEmpty();
    }

    private PangrovaHashJoin buildHashJoin(JoinRelType joinType) {
        RelNode left = buildInput(3);
        RelNode right = buildInput(3);
        RexNode condition = buildEquiCondition(left, right, 0, 0);
        return new PangrovaHashJoin(new HashJoinConfig(
                cluster, left.getTraitSet(), left, right,
                condition, Set.of(), joinType));
    }

    private PangrovaBroadcastJoin buildBroadcastJoin(JoinRelType joinType) {
        RelNode left = buildInput(3);
        RelNode right = buildInput(3);
        RexNode condition = buildEquiCondition(left, right, 0, 0);
        right = new PangrovaExchange(cluster,
                right.getTraitSet().replace(PangrovaDistribution.BROADCAST),
                right, PangrovaDistribution.BROADCAST);
        return new PangrovaBroadcastJoin(new BroadcastJoinConfig(
                cluster, left.getTraitSet(), left, right,
                condition, Set.of(), joinType));
    }

    private PangrovaSortMergeJoin buildSortMergeJoin(JoinRelType joinType) {
        RelNode left = buildInput(3);
        RelNode right = buildInput(3);
        RexNode condition = buildEquiCondition(left, right, 0, 0);
        return new PangrovaSortMergeJoin(new SortMergeJoinConfig(
                cluster, left.getTraitSet(), left, right,
                condition, Set.of(), joinType));
    }

    private PangrovaNestedLoopJoin buildNestedLoopJoin(JoinRelType joinType) {
        RelNode left = buildInput(3);
        RelNode right = buildInput(3);
        RexNode condition = buildEquiCondition(left, right, 0, 0);
        return new PangrovaNestedLoopJoin(new NestedLoopJoinConfig(
                cluster, left.getTraitSet(), left, right,
                condition, Set.of(), joinType));
    }

    private RelNode buildInput(int columnCount) {
        RelDataTypeFactory.Builder typeBuilder = TYPE_FACTORY.builder();
        for (int i = 0; i < columnCount; i++) {
            typeBuilder.add("COL_" + i, SqlTypeName.INTEGER);
        }
        RelDataType rowType = typeBuilder.build();
        ImmutableList.Builder<ImmutableList<RexLiteral>> tuples = ImmutableList.builder();
        ImmutableList.Builder<RexLiteral> row = ImmutableList.builder();
        for (int i = 0; i < columnCount; i++) {
            row.add(rexBuilder.makeExactLiteral(BigDecimal.valueOf(i + 1)));
        }
        tuples.add(row.build());
        return LogicalValues.create(cluster, rowType, tuples.build());
    }

    private RexNode buildEquiCondition(RelNode left, RelNode right,
                                       int leftKeyIdx, int rightKeyIdx) {
        int leftFieldCount = left.getRowType().getFieldCount();
        RexNode leftRef = rexBuilder.makeInputRef(
                left.getRowType().getFieldList().get(leftKeyIdx).getType(), leftKeyIdx);
        RexNode rightRef = rexBuilder.makeInputRef(
                right.getRowType().getFieldList().get(rightKeyIdx).getType(),
                leftFieldCount + rightKeyIdx);
        return rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, leftRef, rightRef);
    }

    private RexNode buildMultiKeyCondition(RelNode left, RelNode right) {
        RexNode cond1 = buildEquiCondition(left, right, 0, 0);
        RexNode cond2 = buildEquiCondition(left, right, 1, 1);
        return rexBuilder.makeCall(SqlStdOperatorTable.AND, cond1, cond2);
    }

    private static int keyOffset(io.substrait.expression.FieldReference reference) {
        return ((io.substrait.expression.FieldReference.StructField)
                reference.segments().get(0)).offset();
    }

    private static HashJoin.JoinType expectedHashJoinType(JoinRelType joinType) {
        return switch (joinType) {
            case INNER -> HashJoin.JoinType.INNER;
            case LEFT -> HashJoin.JoinType.LEFT;
            case RIGHT -> HashJoin.JoinType.RIGHT;
            case FULL -> HashJoin.JoinType.OUTER;
            case SEMI -> HashJoin.JoinType.LEFT_SEMI;
            case ANTI -> HashJoin.JoinType.LEFT_ANTI;
            case ASOF, LEFT_ASOF, LEFT_MARK -> throw new IllegalArgumentException(
                    "Unsupported test join type: " + joinType);
        };
    }

    private static MergeJoin.JoinType expectedMergeJoinType(JoinRelType joinType) {
        return MergeJoin.JoinType.valueOf(expectedHashJoinType(joinType).name());
    }

    private static NestedLoopJoin.JoinType expectedNestedLoopJoinType(JoinRelType joinType) {
        return NestedLoopJoin.JoinType.valueOf(expectedHashJoinType(joinType).name());
    }
}

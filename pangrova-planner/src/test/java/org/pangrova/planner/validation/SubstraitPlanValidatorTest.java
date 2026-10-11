package org.pangrova.planner.validation;

import com.google.protobuf.Any;
import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import io.substrait.expression.ImmutableExpression;
import io.substrait.expression.FieldReference;
import io.substrait.plan.ImmutablePlan;
import io.substrait.plan.ImmutableExecutionBehavior;
import io.substrait.plan.ImmutableRoot;
import io.substrait.plan.Plan;
import io.substrait.relation.ImmutableAggregate;
import io.substrait.relation.ImmutableFilter;
import io.substrait.relation.ImmutableGrouping;
import io.substrait.relation.ImmutableNamedScan;
import io.substrait.relation.ImmutableProject;
import io.substrait.relation.ImmutableSort;
import io.substrait.relation.Extension;
import io.substrait.relation.ExtensionLeaf;
import io.substrait.relation.ExtensionSingle;
import io.substrait.relation.Rel;
import io.substrait.relation.RelProtoConverter;
import io.substrait.type.NamedStruct;
import io.substrait.type.NamedFieldCountingTypeVisitor;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.planner.extension.UnnestRelDetail;

import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link SubstraitPlanValidator}.
 * Covers FULLY_NATIVE, PARTIAL_NATIVE, and JAVA_ONLY scenarios.
 */
class SubstraitPlanValidatorTest {

    private static final TypeCreator NULLABLE = TypeCreator.of(true);

    private SubstraitPlanValidator validator;

    @BeforeEach
    void setUp() {
        validator = new SubstraitPlanValidator(NativeCapabilityRegistry.defaults());
    }

    // --- FULLY_NATIVE scenarios ---

    @Test
    void fullyNativeForSingleReadRel() {
        Plan plan = planOf(namedScan("t1", "id", "name"));

        PlanValidationResult result = validator.validate(plan);

        assertThat(result.status()).isEqualTo(ValidationStatus.FULLY_NATIVE);
        assertThat(result.unsupportedNodes()).isEmpty();
    }

    @Test
    void unknownExtensionLeafDetailRemainsUnsupported() {
        Type.Struct rowType = NULLABLE.struct(NULLABLE.I64);
        Extension.LeafRelDetail unknownDetail = new Extension.LeafRelDetail() {
            @Override public Type.Struct deriveRecordType() { return rowType; }
            @Override public Any toProto(RelProtoConverter converter) {
                return Any.getDefaultInstance();
            }
        };
        PlanValidationResult result = validator.validate(planOf(
                ExtensionLeaf.from(unknownDetail).build()));

        assertThat(result.status()).isEqualTo(ValidationStatus.JAVA_ONLY);
        assertThat(result.unsupportedNodes()).singleElement().satisfies(node -> {
            assertThat(node.kind()).isEqualTo(UnsupportedNode.Kind.RELATION);
            assertThat(node.reason()).contains("detail not supported");
        });
    }

    @Test
    void canonicalUnnestExtensionSingleIsSupported() {
        NamedStruct schema = NamedStruct.of(
                List.of("value"), TypeCreator.REQUIRED.struct(List.of(NULLABLE.I64)));
        Rel unnest = ExtensionSingle.from(
                new UnnestRelDetail("unnest_0", 0, false, false, schema),
                namedScan("t1", "collection")).build();

        assertThat(validator.validate(planOf(unnest)).status())
                .isEqualTo(ValidationStatus.FULLY_NATIVE);
    }

    @Test
    void unknownExtensionSingleDetailRemainsUnsupported() {
        Type.Struct rowType = NULLABLE.struct(NULLABLE.I64);
        Extension.SingleRelDetail unknownDetail = new Extension.SingleRelDetail() {
            @Override public Type.Struct deriveRecordType(Rel input) { return rowType; }
            @Override public Any toProto(RelProtoConverter converter) {
                return Any.getDefaultInstance();
            }
        };
        Rel unknown = ExtensionSingle.from(unknownDetail, namedScan("t1", "id")).build();

        assertThat(validator.validate(planOf(unknown)).unsupportedNodes())
                .singleElement().satisfies(node -> {
                    assertThat(node.kind()).isEqualTo(UnsupportedNode.Kind.RELATION);
                    assertThat(node.reason()).contains("detail not supported");
                });
    }

    @Test
    void fullyNativeForFilterOverRead() {
        Rel scan = namedScan("t1", "id");
        Expression condition = ExpressionCreator.bool(false, true);
        Rel filter = ImmutableFilter.builder()
                .input(scan)
                .condition(condition)
                .build();
        Plan plan = planOf(filter);

        PlanValidationResult result = validator.validate(plan);

        assertThat(result.status()).isEqualTo(ValidationStatus.FULLY_NATIVE);
        assertThat(result.unsupportedNodes()).isEmpty();
    }

    @Test
    void fullyNativeForProjectOverFilterOverRead() {
        Rel scan = namedScan("t1", "id", "name");
        Expression condition = ExpressionCreator.bool(false, true);
        Rel filter = ImmutableFilter.builder()
                .input(scan)
                .condition(condition)
                .build();
        Rel project = ImmutableProject.builder()
                .input(filter)
                .addExpressions(FieldReference.newRootStructReference(0,
                        NULLABLE.STRING))
                .build();
        Plan plan = planOf(project);

        PlanValidationResult result = validator.validate(plan);

        assertThat(result.status()).isEqualTo(ValidationStatus.FULLY_NATIVE);
    }

    @Test
    void nestedConstructorsValidateTheirChildren() {
        Expression field = FieldReference.newRootStructReference(0, NULLABLE.I64);
        Expression list = ImmutableExpression.NestedList.builder()
                .nullable(true).addValues(field).build();
        Expression row = ImmutableExpression.NestedStruct.builder()
                .nullable(true).addFields(list).build();
        Rel project = ImmutableProject.builder().input(namedScan("t1", "id"))
                .addExpressions(row).build();

        assertThat(validator.validate(planOf(project)).unsupportedNodes()).isEmpty();

        NativeCapabilityRegistry withoutFields = NativeCapabilityRegistry.defaults();
        withoutFields.update(Set.of("ReadRel", "ProjectRel"), Set.of("Nested"), Set.of());
        PlanValidationResult rejected = new SubstraitPlanValidator(withoutFields)
                .validate(planOf(project));
        assertThat(rejected.unsupportedNodes()).singleElement().satisfies(node ->
                assertThat(node.nodeType()).isEqualTo("FieldReference"));
    }

    @Test
    void fullyNativeForSortOverRead() {
        Rel scan = namedScan("t1", "id");
        Rel sort = ImmutableSort.builder()
                .input(scan)
                .addSortFields(ImmutableExpression.SortField.builder()
                        .expr(FieldReference.newRootStructReference(0, NULLABLE.I64))
                        .direction(Expression.SortDirection.ASC_NULLS_LAST)
                        .build())
                .build();
        Plan plan = planOf(sort);

        PlanValidationResult result = validator.validate(plan);

        assertThat(result.status()).isEqualTo(ValidationStatus.FULLY_NATIVE);
    }

    @Test
    void fullyNativeForAggregateOverRead() {
        Rel scan = namedScan("t1", "id", "amount");
        Rel agg = ImmutableAggregate.builder()
                .input(scan)
                .addGroupings(ImmutableGrouping.builder()
                        .addExpressions(FieldReference.newRootStructReference(
                                0, NULLABLE.I64))
                        .build())
                .build();
        Plan plan = planOf(agg);

        PlanValidationResult result = validator.validate(plan);

        assertThat(result.status()).isEqualTo(ValidationStatus.FULLY_NATIVE);
    }

    // --- PARTIAL_NATIVE scenarios ---

    @Test
    void partialNativeWhenRegistryMissingSomeRelTypes() {
        // Create a registry that only supports ReadRel.
        NativeCapabilityRegistry limited = NativeCapabilityRegistry.defaults();
        limited.update(Set.of("ReadRel"), Set.of("FieldReference", "Literal"),
                Set.of());
        SubstraitPlanValidator limitedValidator =
                new SubstraitPlanValidator(limited);

        // Plan: Filter(Read) — Filter is unsupported.
        Rel scan = namedScan("t1", "id");
        Rel filter = ImmutableFilter.builder()
                .input(scan)
                .condition(ExpressionCreator.bool(false, true))
                .build();
        Plan plan = planOf(filter);

        PlanValidationResult result = limitedValidator.validate(plan);

        assertThat(result.status()).isEqualTo(ValidationStatus.PARTIAL_NATIVE);
        assertThat(result.unsupportedNodes()).isNotEmpty();
        assertThat(result.unsupportedNodes().stream()
                .map(UnsupportedNode::nodeType))
                .contains("FilterRel");
    }

    // --- JAVA_ONLY scenarios ---

    @Test
    void javaOnlyWhenAllRelsUnsupported() {
        // Empty registry — nothing is supported.
        NativeCapabilityRegistry empty = NativeCapabilityRegistry.defaults();
        empty.update(Set.of(), Set.of(), Set.of());
        SubstraitPlanValidator emptyValidator =
                new SubstraitPlanValidator(empty);

        Rel scan = namedScan("t1", "id");
        Plan plan = planOf(scan);

        PlanValidationResult result = emptyValidator.validate(plan);

        assertThat(result.status()).isEqualTo(ValidationStatus.JAVA_ONLY);
        assertThat(result.unsupportedNodes()).isNotEmpty();
    }

    @Test
    void javaOnlyForDeepTreeWithAllUnsupported() {
        NativeCapabilityRegistry empty = NativeCapabilityRegistry.defaults();
        empty.update(Set.of(), Set.of(), Set.of());
        SubstraitPlanValidator emptyValidator =
                new SubstraitPlanValidator(empty);

        Rel scan = namedScan("t1", "id");
        Rel filter = ImmutableFilter.builder()
                .input(scan)
                .condition(ExpressionCreator.bool(false, true))
                .build();
        Rel project = ImmutableProject.builder()
                .input(filter)
                .addExpressions(FieldReference.newRootStructReference(0,
                        NULLABLE.STRING))
                .build();
        Plan plan = planOf(project);

        PlanValidationResult result = emptyValidator.validate(plan);

        assertThat(result.status()).isEqualTo(ValidationStatus.JAVA_ONLY);
        assertThat(result.unsupportedNodes().size()).isGreaterThanOrEqualTo(3);
    }

    // --- Unsupported nodes list correctness ---

    @Test
    void unsupportedNodesListPopulatedCorrectly() {
        NativeCapabilityRegistry noSort = NativeCapabilityRegistry.defaults();
        noSort.update(
                Set.of("ReadRel", "FilterRel", "ProjectRel", "AggregateRel"),
                Set.of("FieldReference", "Literal", "ScalarFunction", "Cast",
                        "IfThen", "WindowFunction"),
                Set.of());
        SubstraitPlanValidator noSortValidator =
                new SubstraitPlanValidator(noSort);

        Rel scan = namedScan("t1", "id");
        Rel sort = ImmutableSort.builder()
                .input(scan)
                .addSortFields(ImmutableExpression.SortField.builder()
                        .expr(FieldReference.newRootStructReference(0, NULLABLE.I64))
                        .direction(Expression.SortDirection.ASC_NULLS_LAST)
                        .build())
                .build();
        Plan plan = planOf(sort);

        PlanValidationResult result = noSortValidator.validate(plan);

        assertThat(result.status()).isEqualTo(ValidationStatus.PARTIAL_NATIVE);
        assertThat(result.unsupportedNodes()).hasSize(1);
        UnsupportedNode node = result.unsupportedNodes().get(0);
        assertThat(node.nodeType()).isEqualTo("SortRel");
        assertThat(node.depth()).isEqualTo(0);
    }

    // --- ValidationResult immutability ---

    @Test
    void validationResultIsImmutable() {
        Rel scan = namedScan("t1", "id");
        Plan plan = planOf(scan);

        PlanValidationResult result = validator.validate(plan);

        assertThat(result.unsupportedNodes()).isUnmodifiable();
    }

    @Test
    void validationResultWithNullUnsupportedNodesDefaultsToEmpty() {
        PlanValidationResult result = new PlanValidationResult(
                ValidationStatus.FULLY_NATIVE, null);

        assertThat(result.unsupportedNodes()).isEmpty();
    }

    // --- Helpers ---

    private static Rel namedScan(String tableName, String... columns) {
        List<String> names = List.of(columns);
        List<Type> types = names.stream()
                .map(c -> (Type) NULLABLE.I64)
                .toList();
        NamedStruct schema = NamedStruct.of(
                names, TypeCreator.of(true).struct(types));
        return ImmutableNamedScan.builder()
                .addNames(tableName)
                .initialSchema(schema)
                .build();
    }

    private static Plan planOf(Rel rel) {
        int outputNameCount = NamedFieldCountingTypeVisitor.countNames(rel.getRecordType());
        Plan.Root root = ImmutableRoot.builder()
                .input(rel)
                .addAllNames(IntStream.range(0, outputNameCount)
                        .mapToObj(index -> "out_" + index)
                        .toList())
                .build();
        return ImmutablePlan.builder()
                .addRoots(root)
                .executionBehavior(ImmutableExecutionBehavior.builder()
                        .variableEvaluationMode(
                                Plan.ExecutionBehavior.VariableEvaluationMode.PER_PLAN)
                        .build())
                .build();
    }
}

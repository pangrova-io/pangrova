package org.pangrova.planner.substrait;

import io.substrait.expression.ExpressionCreator;
import io.substrait.expression.FieldReference;
import io.substrait.relation.Aggregate;
import io.substrait.relation.ImmutableNamedScan;
import io.substrait.relation.Rel;
import io.substrait.relation.Set;
import io.substrait.type.NamedStruct;
import io.substrait.type.ImmutableNamedStruct;
import io.substrait.type.TypeCreator;
import org.pangrova.planner.validation.NativeCapabilityRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link SetOperationConverter} conditional dedup skip (R10.11).
 */
class SetOperationConverterTest {

    private TypeMapper typeMapper;
    private RelOptCluster cluster;
    private RelDataType rowType;

    @BeforeEach
    void setUp() {
        typeMapper = new TypeMapper();
        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
        RexBuilder rexBuilder = new RexBuilder(typeFactory);
        VolcanoPlanner planner = new VolcanoPlanner();
        cluster = RelOptCluster.create(planner, rexBuilder);
        rowType = typeFactory.builder()
                .add("col1", SqlTypeName.INTEGER)
                .build();
    }

    @Test
    void unionDistinctWithoutRegistryAddsDedupAggregate() {
        SetOperationConverter converter = new SetOperationConverter(typeMapper);
        LogicalUnion union = buildUnionDistinct();

        Rel result = converter.visitUnion(union, this::visitChild);

        assertThat(result).isInstanceOf(Aggregate.class);
    }

    @Test
    void unionDistinctWithNullRegistryAddsDedupAggregate() {
        SetOperationConverter converter = new SetOperationConverter(typeMapper, null);
        LogicalUnion union = buildUnionDistinct();

        Rel result = converter.visitUnion(union, this::visitChild);

        assertThat(result).isInstanceOf(Aggregate.class);
    }

    @Test
    void unionDistinctWithNonSupportingRegistryAddsDedupAggregate() {
        NativeCapabilityRegistry registry = NativeCapabilityRegistry.defaults();
        SetOperationConverter converter = new SetOperationConverter(typeMapper, registry);
        LogicalUnion union = buildUnionDistinct();

        Rel result = converter.visitUnion(union, this::visitChild);

        assertThat(result).isInstanceOf(Aggregate.class);
    }

    @Test
    void unionDistinctWithSupportingRegistrySkipsDedupAggregate() {
        NativeCapabilityRegistry registry = NativeCapabilityRegistry.defaults();
        registry.update(
                java.util.Set.of("SetRel", "SetRel.DISTINCT", "ReadRel"),
                java.util.Set.of("Literal"),
                java.util.Set.of("add"));
        SetOperationConverter converter = new SetOperationConverter(typeMapper, registry);
        LogicalUnion union = buildUnionDistinct();

        Rel result = converter.visitUnion(union, this::visitChild);

        assertThat(result).isInstanceOf(Set.class);
    }

    @Test
    void unionAllNeverAddsDedupRegardlessOfRegistry() {
        NativeCapabilityRegistry registry = NativeCapabilityRegistry.defaults();
        SetOperationConverter converter = new SetOperationConverter(typeMapper, registry);
        LogicalUnion unionAll = LogicalUnion.create(
                List.of(buildValues(), buildValues()), true);

        Rel result = converter.visitUnion(unionAll, this::visitChild);

        assertThat(result).isInstanceOf(Set.class);
        assertThat(((Set) result).getSetOp()).isEqualTo(Set.SetOp.UNION_ALL);
    }

    private LogicalUnion buildUnionDistinct() {
        return LogicalUnion.create(
                List.of(buildValues(), buildValues()), false);
    }

    private LogicalValues buildValues() {
        return LogicalValues.createEmpty(cluster, rowType);
    }

    private Rel visitChild(org.apache.calcite.rel.RelNode node) {
        io.substrait.type.Type intType = TypeCreator.NULLABLE.I32;
        NamedStruct schema = ImmutableNamedStruct.builder()
                .addNames("col1")
                .struct(TypeCreator.NULLABLE.struct(intType))
                .build();
        return ImmutableNamedScan.builder()
                .initialSchema(schema)
                .addNames("test_table")
                .build();
    }
}

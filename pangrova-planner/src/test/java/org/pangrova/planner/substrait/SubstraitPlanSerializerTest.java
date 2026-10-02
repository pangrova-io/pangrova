package org.pangrova.planner.substrait;

import com.google.protobuf.InvalidProtocolBufferException;
import io.substrait.plan.Plan;
import org.apache.calcite.rel.RelNode;
import org.pangrova.planner.PangrovaPlanner;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HexFormat;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for SubstraitPlanSerializer: serialize, round-trip, and invalid input.
 */
class SubstraitPlanSerializerTest {

    private SubstraitPlanSerializer serializer;
    private SubstraitConverter converter;
    private PangrovaPlanner planner;

    @BeforeEach
    void setUp() {
        serializer = new SubstraitPlanSerializer();
        converter = new SubstraitConverter();
        planner = new PangrovaPlanner();
    }

    @Test
    void serializeSelectOneShouldProduceNonEmptyBytes() {
        Plan plan = buildPlan("SELECT 1");
        byte[] bytes = serializer.serialize(plan);
        assertThat(bytes).isNotNull().isNotEmpty();
    }

    @Test
    void roundTripShouldPreservePlanStructure() throws Exception {
        Plan original = buildPlan("SELECT 1");
        byte[] bytes = serializer.serialize(original);
        var parsed = io.substrait.proto.Plan.parseFrom(bytes);
        var restored = io.substrait.proto.Plan.parseFrom(parsed.toByteArray());

        assertThat(restored.getRelationsList()).hasSameSizeAs(original.getRoots());
        assertThat(restored.getRelations(0).getRoot().getNamesList())
                .isEqualTo(original.getRoots().get(0).getNames());
        assertThat(restored).isEqualTo(parsed);
        var read = findRead(restored.getRelations(0).getRoot().getInput());
        assertThat(read.hasBaseSchema()).isTrue();
        assertThat(read.getBaseSchema().getStruct().getTypesCount()).isOne();
        assertThat(read.getBaseSchema().getStruct().getTypes(0).hasI32()).isTrue();
        assertThat(read.getVirtualTable().getExpressionsCount()).isOne();
        var field = read.getVirtualTable().getExpressions(0).getFields(0);
        assertThat(field.hasLiteral()).isTrue();
        assertThat(field.getLiteral().hasI32()).isTrue();
        assertThat(field.getLiteral().getI32()).isOne();
    }

    @Test
    void generatedProtoParserRejectsInvalidBytes() {
        byte[] garbage = {0x00, 0x01, 0x02, (byte) 0xFF};
        assertThatThrownBy(() -> io.substrait.proto.Plan.parseFrom(garbage))
                .isInstanceOf(InvalidProtocolBufferException.class);
    }

    @Test
    void serializeFetchUsesCurrentExpressionFields() throws Exception {
        Plan plan = buildPlan("SELECT 1 AS id LIMIT 1 OFFSET 1");

        io.substrait.proto.Plan proto = io.substrait.proto.Plan.parseFrom(
                serializer.serialize(plan));
        io.substrait.proto.FetchRel fetch = findFetch(
                proto.getRelations(0).getRoot().getInput()).orElseThrow();

        assertThat(fetch.hasOffsetExpr()).isTrue();
        assertThat(fetch.getOffsetExpr().getLiteral().getI64()).isOne();
        assertThat(fetch.hasCountExpr()).isTrue();
        assertThat(fetch.getCountExpr().getLiteral().getI64()).isOne();
        assertThat(fetch.hasOffset()).isFalse();
        assertThat(fetch.hasCount()).isFalse();
        var reparsed = io.substrait.proto.Plan.parseFrom(proto.toByteArray());
        assertThat(reparsed.getRelationsList()).hasSize(1);
        assertThat(reparsed).isEqualTo(proto);
        assertThat(findFetch(reparsed.getRelations(0).getRoot().getInput()).orElseThrow())
                .isEqualTo(fetch);
    }

    @Test
    void typedValuesSurviveCanonicalProtoReparseWithoutLosingNullOrDecimalBytes() throws Exception {
        Plan plan = buildPlan("SELECT CAST(1.23 AS DECIMAL(10,2)) AS short_value, "
                + "CAST(-0.77 AS DECIMAL(25,4)) AS long_value, "
                + "CAST(NULL AS DECIMAL(25,4)) AS missing_value");
        var parsed = io.substrait.proto.Plan.parseFrom(serializer.serialize(plan));
        var reparsed = io.substrait.proto.Plan.parseFrom(parsed.toByteArray());
        assertThat(reparsed).isEqualTo(parsed);
        var read = findRead(reparsed.getRelations(0).getRoot().getInput());
        assertThat(read.hasBaseSchema()).isTrue();
        assertThat(read.getBaseSchema().getStruct().getTypesCount()).isEqualTo(3);
        var types = read.getBaseSchema().getStruct().getTypesList();
        assertDecimalType(types.get(0), 10, 2, false);
        assertDecimalType(types.get(1), 25, 4, false);
        assertDecimalType(types.get(2), 25, 4, true);
        var table = read.getVirtualTable();
        var oldValuesField = table.getDescriptorForType().findFieldByName("values");
        assertThat(table.getRepeatedFieldCount(oldValuesField)).isZero();
        assertThat(table.getExpressionsCount()).isOne();
        var row = table.getExpressions(0);
        assertThat(row.getFieldsCount()).isEqualTo(3);
        var shortValue = row.getFields(0).getLiteral().getDecimal();
        assertThat(shortValue.getPrecision()).isEqualTo(10);
        assertThat(shortValue.getScale()).isEqualTo(2);
        assertThat(shortValue.getValue().toByteArray()).isEqualTo(
                HexFormat.of().parseHex("7b000000000000000000000000000000"));
        var longValue = row.getFields(1).getLiteral().getDecimal();
        assertThat(longValue.getPrecision()).isEqualTo(25);
        assertThat(longValue.getScale()).isEqualTo(4);
        assertThat(longValue.getValue().toByteArray()).isEqualTo(
                HexFormat.of().parseHex("ece1ffffffffffffffffffffffffffff"));
        var missing = row.getFields(2).getLiteral();
        assertThat(missing.hasNull()).isTrue();
        assertThat(missing.getNull()).isEqualTo(read.getBaseSchema().getStruct().getTypes(2));
        assertThat(missing.getNull().getDecimal().getPrecision()).isEqualTo(25);
        assertThat(missing.getNull().getDecimal().getScale()).isEqualTo(4);
        assertThat(missing.getNull().getDecimal().getNullability())
                .isEqualTo(io.substrait.proto.Type.Nullability.NULLABILITY_NULLABLE);
    }

    private static void assertDecimalType(
            io.substrait.proto.Type type, int precision, int scale, boolean nullable) {
        assertThat(type.hasDecimal()).isTrue();
        assertThat(type.getDecimal().getPrecision()).isEqualTo(precision);
        assertThat(type.getDecimal().getScale()).isEqualTo(scale);
        assertThat(type.getDecimal().getNullability()).isEqualTo(nullable
                ? io.substrait.proto.Type.Nullability.NULLABILITY_NULLABLE
                : io.substrait.proto.Type.Nullability.NULLABILITY_REQUIRED);
    }

    private static io.substrait.proto.ReadRel findRead(io.substrait.proto.Rel relation) {
        return switch (relation.getRelTypeCase()) {
            case READ -> relation.getRead();
            case PROJECT -> findRead(relation.getProject().getInput());
            case FILTER -> findRead(relation.getFilter().getInput());
            case SORT -> findRead(relation.getSort().getInput());
            case FETCH -> findRead(relation.getFetch().getInput());
            default -> throw new AssertionError("Expected VALUES source, not " + relation.getRelTypeCase());
        };
    }

    private static Optional<io.substrait.proto.FetchRel> findFetch(
            io.substrait.proto.Rel rel) {
        return switch (rel.getRelTypeCase()) {
            case FETCH -> Optional.of(rel.getFetch());
            case PROJECT -> findFetch(rel.getProject().getInput());
            case FILTER -> findFetch(rel.getFilter().getInput());
            case SORT -> findFetch(rel.getSort().getInput());
            default -> Optional.empty();
        };
    }

    private Plan buildPlan(String sql) {
        RelNode relNode = planner.plan(sql).plan();
        return converter.convert(relNode);
    }
}

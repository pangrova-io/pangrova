package org.pangrova.connector.iceberg;

import io.substrait.proto.ExtendedExpression;
import io.substrait.proto.ExpressionReference;
import org.pangrova.connector.iceberg.handle.IcebergSplit;
import org.pangrova.connector.iceberg.read.IcebergExpressionToSubstrait;
import org.pangrova.protocol.v1.SplitConstraintInfo;
import org.pangrova.protocol.v1.SplitInfo;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Binder;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test verifying that the SplitConstraint predicate injection pipeline
 * correctly converts Iceberg expressions to Substrait and attaches them to splits.
 *
 * <p>Tests the full pipeline:
 * Iceberg Expression → IcebergExpressionToSubstrait → byte[] → IcebergSplit.residualPredicate
 * → SplitConstraintInfo.predicate (proto).</p>
 *
 * <p>Validates: Requirements 1.2</p>
 */
class IcebergSplitPredicateInjectionIntegrationTest {

    private IcebergExpressionToSubstrait expressionConverter;

    /**
     * Schema mimicking TPC-H lineitem relevant columns for Q6 filter:
     * l_quantity < 24 AND l_discount BETWEEN 0.05 AND 0.07
     */
    private static final Schema LINEITEM_SCHEMA = new Schema(
            Types.NestedField.required(1, "l_orderkey", Types.LongType.get()),
            Types.NestedField.optional(2, "l_quantity", Types.DoubleType.get()),
            Types.NestedField.optional(3, "l_discount", Types.DoubleType.get()),
            Types.NestedField.optional(4, "l_shipdate", Types.DateType.get()),
            Types.NestedField.optional(5, "l_extendedprice", Types.DoubleType.get()));

    @BeforeEach
    void setUp() {
        expressionConverter = new IcebergExpressionToSubstrait();
    }

    /**
     * Verifies that IcebergExpressionToSubstrait converts a bound comparison
     * predicate (l_quantity < 24) into a non-empty, valid Substrait ExtendedExpression.
     */
    @Test
    void converterProducesNonEmptySubstraitForBoundPredicate() throws Exception {
        org.apache.iceberg.expressions.Expression residual =
                Binder.bind(LINEITEM_SCHEMA.asStruct(),
                        Expressions.lessThan("l_quantity", 24.0), true);

        Optional<byte[]> result = expressionConverter.convert(residual, LINEITEM_SCHEMA);

        assertThat(result).isPresent();
        byte[] predicateBytes = result.get();
        assertThat(predicateBytes.length)
                .as("Substrait predicate bytes should be non-empty")
                .isGreaterThan(0);

        ExtendedExpression extExpr = ExtendedExpression.parseFrom(predicateBytes);
        assertThat(extExpr.getReferredExprCount())
                .as("ExtendedExpression should contain at least one expression reference")
                .isGreaterThan(0);

        assertThat(extExpr.hasBaseSchema()).isTrue();
        assertThat(extExpr.getBaseSchema().getNamesList())
                .contains("l_orderkey", "l_quantity", "l_discount",
                        "l_shipdate", "l_extendedprice");

        assertThat(extExpr.getExtensionUrisCount())
                .as("ExtendedExpression should declare extension URIs")
                .isGreaterThan(0);

        ExpressionReference exprRef = extExpr.getReferredExpr(0);
        assertThat(exprRef.hasExpression()).isTrue();
    }

    /**
     * Verifies that a compound AND filter (TPC-H Q6 style) produces a valid
     * Substrait expression with nested boolean function calls.
     */
    @Test
    void converterProducesValidSubstraitForCompoundAndPredicate() throws Exception {
        org.apache.iceberg.expressions.Expression residual =
                Binder.bind(LINEITEM_SCHEMA.asStruct(),
                        Expressions.and(
                                Expressions.lessThan("l_quantity", 24.0),
                                Expressions.greaterThanOrEqual("l_discount", 0.05)),
                        true);

        Optional<byte[]> result = expressionConverter.convert(residual, LINEITEM_SCHEMA);

        assertThat(result).isPresent();
        byte[] predicateBytes = result.get();

        ExtendedExpression extExpr = ExtendedExpression.parseFrom(predicateBytes);
        assertThat(extExpr.getReferredExprCount()).isGreaterThan(0);

        ExpressionReference exprRef = extExpr.getReferredExpr(0);
        assertThat(exprRef.getExpression().hasScalarFunction()).isTrue();
    }

    /**
     * Verifies that an IcebergSplit with non-empty residualPredicate correctly
     * serializes the predicate into SplitConstraintInfo.predicate in the proto.
     */
    @Test
    void splitWithPredicateSerializesToProtoSplitConstraint() throws Exception {
        org.apache.iceberg.expressions.Expression residual =
                Binder.bind(LINEITEM_SCHEMA.asStruct(),
                        Expressions.and(
                                Expressions.lessThan("l_quantity", 24.0),
                                Expressions.greaterThanOrEqual("l_discount", 0.05)),
                        true);
        byte[] predicateBytes = expressionConverter.convert(residual, LINEITEM_SCHEMA)
                .orElseThrow();

        IcebergSplit split = new IcebergSplit(
                "tpch.lineitem",
                "/data/lineitem-001.parquet", 0L, 10240L, 10240L, "PARQUET",
                Map.of(),
                100L, 1L, 0, "DATA", "FULL", 0L, 0L, List.of(),
                List.of(), List.of(), predicateBytes);

        assertThat(split.residualPredicate())
                .as("IcebergSplit should carry the residual predicate bytes")
                .isNotEmpty()
                .isEqualTo(predicateBytes);

        byte[] protoBytes = split.toSplitInfoProto();
        SplitInfo info = SplitInfo.parseFrom(protoBytes);

        assertThat(info.hasSplitConstraint()).isTrue();
        SplitConstraintInfo constraint = info.getSplitConstraint();
        assertThat(constraint.getPredicate().size())
                .as("SplitConstraintInfo.predicate should be non-empty")
                .isGreaterThan(0);
        assertThat(constraint.getPredicate().toByteArray())
                .isEqualTo(predicateBytes);

        ExtendedExpression extExpr = ExtendedExpression.parseFrom(constraint.getPredicate());
        assertThat(extExpr.getReferredExprCount()).isGreaterThan(0);
    }

    /**
     * Verifies that an IcebergSplit without a residual predicate does NOT
     * produce a SplitConstraintInfo in the proto (when also no partition values).
     */
    @Test
    void splitWithoutPredicateOmitsSplitConstraintInProto() throws Exception {
        IcebergSplit split = new IcebergSplit(
                "tpch.lineitem",
                "/data/lineitem-no-filter.parquet", 0L, 4096L, 4096L, "PARQUET",
                Map.of(),
                100L, 1L, 0, "DATA", "FULL", 0L, 0L, List.of(),
                List.of(), List.of(), new byte[0]);

        byte[] protoBytes = split.toSplitInfoProto();
        SplitInfo info = SplitInfo.parseFrom(protoBytes);

        assertThat(info.hasSplitConstraint()).isFalse();
    }

    /**
     * Verifies that the converter returns empty for trivial expressions (alwaysTrue).
     */
    @Test
    void converterReturnsEmptyForAlwaysTrueExpression() {
        Optional<byte[]> result = expressionConverter.convert(
                Expressions.alwaysTrue(), LINEITEM_SCHEMA);
        assertThat(result).isEmpty();
    }
}

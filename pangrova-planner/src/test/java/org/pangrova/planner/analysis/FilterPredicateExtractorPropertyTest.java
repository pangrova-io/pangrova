package org.pangrova.planner.analysis;

import org.pangrova.connector.api.enums.ContainmentMode;
import org.pangrova.connector.api.index.predicate.ContainsElement;
import org.pangrova.connector.api.index.predicate.EqualTo;
import org.pangrova.connector.api.index.predicate.InList;
import org.pangrova.connector.api.index.predicate.IndexPredicate;
import org.pangrova.connector.api.index.predicate.IsNotNull;
import org.pangrova.connector.api.index.predicate.IsNull;
import org.pangrova.connector.api.index.predicate.LikePattern;
import org.pangrova.connector.api.index.predicate.RangeQuery;
import org.pangrova.connector.api.enums.SpatialOp;
import org.pangrova.connector.api.index.predicate.SpatialQuery;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property tests for {@link FilterPredicateExtractor}.
 *
 * <p>Manually constructs Calcite type system objects (no mock frameworks).
 * Verifies extraction of all supported predicate types from Filter conditions.</p>
 *
 * <p><b>Validates: Requirements R0C.1, R0C.2, R0C.3, R0C.4</b></p>
 */
class FilterPredicateExtractorPropertyTest {

    private static final RelDataTypeFactory TYPE_FACTORY = new JavaTypeFactoryImpl();
    private static final RexBuilder REX = new RexBuilder(TYPE_FACTORY);
    private static final RelOptCluster CLUSTER = RelOptCluster.create(
            new VolcanoPlanner(), REX);

    /** Standard table: (user_id BIGINT, name VARCHAR, amount DOUBLE, status VARCHAR). */
    private static final RelDataType ROW_TYPE = TYPE_FACTORY.builder()
            .add("user_id", SqlTypeName.BIGINT)
            .add("name", SqlTypeName.VARCHAR, 255)
            .add("amount", SqlTypeName.DOUBLE)
            .add("status", SqlTypeName.VARCHAR, 50)
            .build();

    private static final LogicalTableScan SCAN = LogicalTableScan.create(
            CLUSTER, new MinimalTable("test_table", ROW_TYPE), List.of());

    @Test
    void singleScanHintsDoNotEscapeToAnotherOccurrenceOfTheSameTable() {
        RexNode condition = REX.makeCall(SqlStdOperatorTable.EQUALS,
                REX.makeInputRef(ROW_TYPE.getFieldList().get(0).getType(), 0),
                REX.makeBigintLiteral(BigDecimal.valueOf(42)));
        var filtered = LogicalFilter.create(SCAN, condition);
        var selection = PlanSplitSourceExtractor.singleScanPredicates(filtered).orElseThrow();
        assertThat(selection.table().table()).isEqualTo("test_table");
        assertThat(selection.predicates()).containsExactlyElementsOf(FilterPredicateExtractor.extract(filtered));
        var union = org.apache.calcite.rel.logical.LogicalUnion.create(List.of(filtered, SCAN), true);
        assertThat(PlanSplitSourceExtractor.singleScanPredicates(union)).isEmpty();
    }

    // --- 7.2: Existing predicate types ---

    @Test
    void equalTo_columnEqualsLiteral() {
        RexNode condition = REX.makeCall(SqlStdOperatorTable.EQUALS,
                REX.makeInputRef(ROW_TYPE.getFieldList().get(0).getType(), 0),
                REX.makeBigintLiteral(BigDecimal.valueOf(42)));
        List<IndexPredicate> result = extract(condition);
        assertThat(result).hasSize(1);
        assertThat(result.get(0)).isInstanceOf(EqualTo.class);
        EqualTo eq = (EqualTo) result.get(0);
        assertThat(eq.column()).isEqualTo("user_id");
        assertThat(eq.value()).isEqualTo(42L);
    }

    @Test
    void equalTo_stringLiteralUsesUnquotedValue() {
        RexNode condition = REX.makeCall(SqlStdOperatorTable.EQUALS,
                REX.makeInputRef(ROW_TYPE.getFieldList().get(3).getType(), 3),
                REX.makeLiteral("ACTIVE"));
        List<IndexPredicate> result = extract(condition);
        assertThat(result).hasSize(1);
        EqualTo eq = (EqualTo) result.get(0);
        assertThat(eq.column()).isEqualTo("status");
        assertThat(eq.value()).isEqualTo("ACTIVE");
    }

    @Test
    void rangeQuery_lessThan() {
        RexNode condition = REX.makeCall(SqlStdOperatorTable.LESS_THAN,
                REX.makeInputRef(ROW_TYPE.getFieldList().get(2).getType(), 2),
                REX.makeApproxLiteral(BigDecimal.valueOf(100.0)));
        List<IndexPredicate> result = extract(condition);
        assertThat(result).hasSize(1);
        RangeQuery rq = (RangeQuery) result.get(0);
        assertThat(rq.column()).isEqualTo("amount");
        assertThat(rq.lower()).isNull();
        assertThat(rq.upper()).isEqualTo(100.0);
        assertThat(rq.lowerInclusive()).isFalse();
        assertThat(rq.upperInclusive()).isFalse();
    }

    @Test
    void rangeQuery_lessThanOrEqual() {
        RexNode condition = REX.makeCall(SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
                REX.makeInputRef(ROW_TYPE.getFieldList().get(2).getType(), 2),
                REX.makeApproxLiteral(BigDecimal.valueOf(200.0)));
        List<IndexPredicate> result = extract(condition);
        assertThat(result).hasSize(1);
        RangeQuery rq = (RangeQuery) result.get(0);
        assertThat(rq.column()).isEqualTo("amount");
        assertThat(rq.lower()).isNull();
        assertThat(rq.upper()).isEqualTo(200.0);
        assertThat(rq.upperInclusive()).isTrue();
    }

    @Test
    void rangeQuery_greaterThan() {
        RexNode condition = REX.makeCall(SqlStdOperatorTable.GREATER_THAN,
                REX.makeInputRef(ROW_TYPE.getFieldList().get(2).getType(), 2),
                REX.makeApproxLiteral(BigDecimal.valueOf(50.0)));
        List<IndexPredicate> result = extract(condition);
        assertThat(result).hasSize(1);
        RangeQuery rq = (RangeQuery) result.get(0);
        assertThat(rq.column()).isEqualTo("amount");
        assertThat(rq.lower()).isEqualTo(50.0);
        assertThat(rq.upper()).isNull();
        assertThat(rq.lowerInclusive()).isFalse();
    }

    @Test
    void rangeQuery_greaterThanOrEqual() {
        RexNode condition = REX.makeCall(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
                REX.makeInputRef(ROW_TYPE.getFieldList().get(2).getType(), 2),
                REX.makeApproxLiteral(BigDecimal.valueOf(75.0)));
        List<IndexPredicate> result = extract(condition);
        assertThat(result).hasSize(1);
        RangeQuery rq = (RangeQuery) result.get(0);
        assertThat(rq.column()).isEqualTo("amount");
        assertThat(rq.lower()).isEqualTo(75.0);
        assertThat(rq.lowerInclusive()).isTrue();
    }

    @Test
    void isNull_extraction() {
        RexNode condition = REX.makeCall(SqlStdOperatorTable.IS_NULL,
                REX.makeInputRef(ROW_TYPE.getFieldList().get(1).getType(), 1));
        List<IndexPredicate> result = extract(condition);
        assertThat(result).hasSize(1);
        assertThat(result.get(0)).isInstanceOf(IsNull.class);
        assertThat(result.get(0).column()).isEqualTo("name");
    }

    @Test
    void isNotNull_extraction() {
        RexNode condition = REX.makeCall(SqlStdOperatorTable.IS_NOT_NULL,
                REX.makeInputRef(ROW_TYPE.getFieldList().get(1).getType(), 1));
        List<IndexPredicate> result = extract(condition);
        assertThat(result).hasSize(1);
        assertThat(result.get(0)).isInstanceOf(IsNotNull.class);
        assertThat(result.get(0).column()).isEqualTo("name");
    }

    // Note: InList extraction via SqlKind.IN is not testable with Calcite 1.34.
    // Calcite 1.34 represents IN(col, v1, v2, v3) as SEARCH(col, Sarg[v1,v2,v3])
    // or RexSubQuery.in() for subqueries. The extractor's IN branch handles a flat
    // RexCall(IN, [col, lit1, lit2, ...]) which Calcite 1.34 cannot construct.

    @Test
    void andConjunction_decomposesIntoMultiplePredicates() {
        RexNode eq = REX.makeCall(SqlStdOperatorTable.EQUALS,
                REX.makeInputRef(ROW_TYPE.getFieldList().get(0).getType(), 0),
                REX.makeBigintLiteral(BigDecimal.TEN));
        RexNode gt = REX.makeCall(SqlStdOperatorTable.GREATER_THAN,
                REX.makeInputRef(ROW_TYPE.getFieldList().get(2).getType(), 2),
                REX.makeApproxLiteral(BigDecimal.valueOf(50.0)));
        RexNode and = REX.makeCall(SqlStdOperatorTable.AND, eq, gt);
        List<IndexPredicate> result = extract(and);
        assertThat(result).hasSize(2);
        assertThat(result.get(0)).isInstanceOf(EqualTo.class);
        assertThat(result.get(1)).isInstanceOf(RangeQuery.class);
    }

    @Test
    void swappedOperands_literalOnLeft() {
        RexNode condition = REX.makeCall(SqlStdOperatorTable.EQUALS,
                REX.makeBigintLiteral(BigDecimal.valueOf(42)),
                REX.makeInputRef(ROW_TYPE.getFieldList().get(0).getType(), 0));
        List<IndexPredicate> result = extract(condition);
        assertThat(result).hasSize(1);
        EqualTo eq = (EqualTo) result.get(0);
        assertThat(eq.column()).isEqualTo("user_id");
        assertThat(eq.value()).isEqualTo(42L);
    }

    @Test
    void swappedOperands_lessThan_literalOnLeft() {
        RexNode condition = REX.makeCall(SqlStdOperatorTable.LESS_THAN,
                REX.makeApproxLiteral(BigDecimal.valueOf(100.0)),
                REX.makeInputRef(ROW_TYPE.getFieldList().get(2).getType(), 2));
        List<IndexPredicate> result = extract(condition);
        assertThat(result).hasSize(1);
        RangeQuery rq = (RangeQuery) result.get(0);
        assertThat(rq.column()).isEqualTo("amount");
        assertThat(rq.lower()).isEqualTo(100.0);
        assertThat(rq.upper()).isNull();
        assertThat(rq.lowerInclusive()).isFalse();
    }

    @Test
    void bigDecimalCoercion_integerColumn() {
        RexNode condition = REX.makeCall(SqlStdOperatorTable.EQUALS,
                REX.makeInputRef(ROW_TYPE.getFieldList().get(0).getType(), 0),
                REX.makeBigintLiteral(BigDecimal.valueOf(7)));
        List<IndexPredicate> result = extract(condition);

        assertThat(result).hasSize(1);
        EqualTo eq = (EqualTo) result.get(0);
        assertThat(eq.value()).isInstanceOf(Long.class);
        assertThat(eq.value()).isEqualTo(7L);
    }

    @Test
    void bigDecimalCoercion_doubleColumn() {
        RelDataType doubleType = ROW_TYPE.getFieldList().get(2).getType();
        RexNode condition = REX.makeCall(SqlStdOperatorTable.EQUALS,
                REX.makeInputRef(doubleType, 2),
                REX.makeApproxLiteral(BigDecimal.valueOf(3.14)));
        List<IndexPredicate> result = extract(condition);

        assertThat(result).hasSize(1);
        EqualTo eq = (EqualTo) result.get(0);
        assertThat(eq.value()).isInstanceOf(Double.class);
        assertThat((Double) eq.value()).isEqualTo(3.14);
    }

    // --- 7.3: New predicate types ---

    @Test
    void likePattern_prefixPattern() {
        RexNode condition = REX.makeCall(SqlStdOperatorTable.LIKE,
                REX.makeInputRef(ROW_TYPE.getFieldList().get(1).getType(), 1),
                REX.makeLiteral("Ali%"));
        List<IndexPredicate> result = extract(condition);

        assertThat(result).hasSize(1);
        LikePattern lp = (LikePattern) result.get(0);
        assertThat(lp.column()).isEqualTo("name");
        assertThat(lp.pattern()).isEqualTo("Ali%");
    }

    @Test
    void likePattern_substringPattern() {
        RexNode condition = REX.makeCall(SqlStdOperatorTable.LIKE,
                REX.makeInputRef(ROW_TYPE.getFieldList().get(1).getType(), 1),
                REX.makeLiteral("%ether%"));
        List<IndexPredicate> result = extract(condition);

        assertThat(result).hasSize(1);
        LikePattern lp = (LikePattern) result.get(0);
        assertThat(lp.column()).isEqualTo("name");
        assertThat(lp.pattern()).isEqualTo("%ether%");
    }

    @Test
    void likePattern_nonLiteralPattern_producesNoPredicate() {
        RexNode condition = REX.makeCall(SqlStdOperatorTable.LIKE,
                REX.makeInputRef(ROW_TYPE.getFieldList().get(1).getType(), 1),
                REX.makeInputRef(ROW_TYPE.getFieldList().get(3).getType(), 3));
        List<IndexPredicate> result = extract(condition);

        assertThat(result).isEmpty();
    }

    @Test
    void spatialQuery_intersects() {
        RelDataType spatialRow = TYPE_FACTORY.builder()
                .add("geo", SqlTypeName.VARCHAR, 255)
                .build();
        LogicalTableScan spatialScan = LogicalTableScan.create(
                CLUSTER, new MinimalTable("spatial_table", spatialRow), List.of());

        SqlFunction stIntersects = customFunction("ST_INTERSECTS");
        RexNode condition = REX.makeCall(stIntersects,
                REX.makeInputRef(spatialRow.getFieldList().get(0).getType(), 0),
                REX.makeApproxLiteral(BigDecimal.valueOf(1.0)),
                REX.makeApproxLiteral(BigDecimal.valueOf(2.0)),
                REX.makeApproxLiteral(BigDecimal.valueOf(3.0)),
                REX.makeApproxLiteral(BigDecimal.valueOf(4.0)));
        List<IndexPredicate> result = extractWith(condition, spatialScan);

        assertThat(result).hasSize(1);
        SpatialQuery sq = (SpatialQuery) result.get(0);
        assertThat(sq.column()).isEqualTo("geo");
        assertThat(sq.minX()).isEqualTo(1.0);
        assertThat(sq.minY()).isEqualTo(2.0);
        assertThat(sq.maxX()).isEqualTo(3.0);
        assertThat(sq.maxY()).isEqualTo(4.0);
        assertThat(sq.op()).isEqualTo(SpatialOp.INTERSECTS);
    }

    @Test
    void spatialQuery_withinAndContains() {
        RelDataType spatialRow = TYPE_FACTORY.builder()
                .add("loc", SqlTypeName.VARCHAR, 255).build();
        LogicalTableScan spatialScan = LogicalTableScan.create(
                CLUSTER, new MinimalTable("t", spatialRow), List.of());
        RexNode ref = REX.makeInputRef(spatialRow.getFieldList().get(0).getType(), 0);

        for (var entry : List.of(
                new Object[]{"ST_WITHIN", SpatialOp.WITHIN},
                new Object[]{"ST_CONTAINS", SpatialOp.CONTAINS})) {
            String fname = (String) entry[0];
            SpatialOp expectedOp = (SpatialOp) entry[1];
            RexNode cond = REX.makeCall(customFunction(fname), ref,
                    REX.makeApproxLiteral(BigDecimal.ONE),
                    REX.makeApproxLiteral(BigDecimal.valueOf(2)),
                    REX.makeApproxLiteral(BigDecimal.valueOf(3)),
                    REX.makeApproxLiteral(BigDecimal.valueOf(4)));
            List<IndexPredicate> result = extractWith(cond, spatialScan);
            assertThat(result).hasSize(1);
            assertThat(((SpatialQuery) result.get(0)).op()).isEqualTo(expectedOp);
        }
    }

    @Test
    void spatialQuery_nonNumericCoordinate_producesNoPredicate() {
        RelDataType spatialRow = TYPE_FACTORY.builder()
                .add("geo", SqlTypeName.VARCHAR, 255).build();
        LogicalTableScan spatialScan = LogicalTableScan.create(
                CLUSTER, new MinimalTable("t", spatialRow), List.of());
        RexNode condition = REX.makeCall(customFunction("ST_INTERSECTS"),
                REX.makeInputRef(spatialRow.getFieldList().get(0).getType(), 0),
                REX.makeLiteral("not_a_number"),
                REX.makeApproxLiteral(BigDecimal.valueOf(2.0)),
                REX.makeApproxLiteral(BigDecimal.valueOf(3.0)),
                REX.makeApproxLiteral(BigDecimal.valueOf(4.0)));
        List<IndexPredicate> result = extractWith(condition, spatialScan);
        assertThat(result).isEmpty();
    }

    @Test
    void containsElement_anyMode() {
        SqlFunction arrayContains = customFunction("ARRAY_CONTAINS");
        RexNode condition = REX.makeCall(arrayContains,
                REX.makeInputRef(ROW_TYPE.getFieldList().get(1).getType(), 1),
                REX.makeLiteral("alpha"));
        List<IndexPredicate> result = extract(condition);

        assertThat(result).hasSize(1);
        ContainsElement ce = (ContainsElement) result.get(0);
        assertThat(ce.column()).isEqualTo("name");
        assertThat(ce.elements()).containsExactly("alpha");
        assertThat(ce.mode()).isEqualTo(ContainmentMode.CONTAINS_ANY);
    }

    @Test
    void containsElement_allMode() {
        SqlFunction arrayContainsAll = customFunction("ARRAY_CONTAINS_ALL");
        RexNode condition = REX.makeCall(arrayContainsAll,
                REX.makeInputRef(ROW_TYPE.getFieldList().get(1).getType(), 1),
                REX.makeLiteral("a"),
                REX.makeLiteral("b"));
        List<IndexPredicate> result = extract(condition);

        assertThat(result).hasSize(1);
        ContainsElement ce = (ContainsElement) result.get(0);
        assertThat(ce.column()).isEqualTo("name");
        assertThat(ce.elements()).containsExactly("a", "b");
        assertThat(ce.mode()).isEqualTo(ContainmentMode.CONTAINS_ALL);
    }

    @Test
    void containsElement_nonColumnFirstArg_producesNoPredicate() {
        SqlFunction ac = customFunction("ARRAY_CONTAINS");
        RexNode condition = REX.makeCall(ac,
                REX.makeLiteral("literal_value"),
                REX.makeLiteral("alpha"));
        List<IndexPredicate> result = extract(condition);

        assertThat(result).isEmpty();
    }

    // --- 7.4: Negative / edge cases ---

    @Test
    void sameColumnOr_producesInList() {
        RexNode left = REX.makeCall(SqlStdOperatorTable.EQUALS,
                REX.makeInputRef(ROW_TYPE.getFieldList().get(0).getType(), 0),
                REX.makeBigintLiteral(BigDecimal.ONE));
        RexNode right = REX.makeCall(SqlStdOperatorTable.EQUALS,
                REX.makeInputRef(ROW_TYPE.getFieldList().get(0).getType(), 0),
                REX.makeBigintLiteral(BigDecimal.valueOf(2)));
        RexNode or = REX.makeCall(SqlStdOperatorTable.OR, left, right);
        List<IndexPredicate> result = extract(or);

        assertThat(result).hasSize(1);
        assertThat(result.get(0)).isInstanceOf(InList.class);
        InList inList = (InList) result.get(0);
        assertThat(inList.column()).isEqualTo("user_id");
        assertThat(inList.values()).containsExactlyInAnyOrder(1L, 2L);
    }

    @Test
    void sameColumnStringOrUsesUnquotedValues() {
        RexNode status = REX.makeInputRef(ROW_TYPE.getFieldList().get(3).getType(), 3);
        RexNode active = REX.makeCall(SqlStdOperatorTable.EQUALS,
                status, REX.makeLiteral("ACTIVE"));
        RexNode inactive = REX.makeCall(SqlStdOperatorTable.EQUALS,
                status, REX.makeLiteral("INACTIVE"));
        List<IndexPredicate> result = extract(REX.makeCall(SqlStdOperatorTable.OR, active, inactive));

        assertThat(result).hasSize(1);
        InList inList = (InList) result.get(0);
        assertThat(inList.column()).isEqualTo("status");
        assertThat(inList.values()).containsExactlyInAnyOrder("ACTIVE", "INACTIVE");
    }

    @Test
    void crossColumnOr_producesNoPredicate() {
        RexNode left = REX.makeCall(SqlStdOperatorTable.EQUALS,
                REX.makeInputRef(ROW_TYPE.getFieldList().get(0).getType(), 0),
                REX.makeBigintLiteral(BigDecimal.ONE));
        RexNode right = REX.makeCall(SqlStdOperatorTable.EQUALS,
                REX.makeInputRef(ROW_TYPE.getFieldList().get(2).getType(), 2),
                REX.makeApproxLiteral(BigDecimal.valueOf(3.0)));
        RexNode or = REX.makeCall(SqlStdOperatorTable.OR, left, right);
        List<IndexPredicate> result = extract(or);

        assertThat(result).isEmpty();
    }

    @Test
    void emptyFilter_noCondition() {
        RexNode trueCondition = REX.makeLiteral(true);
        LogicalFilter filter = LogicalFilter.create(SCAN, trueCondition);
        List<IndexPredicate> result = FilterPredicateExtractor.extract(filter);
        assertThat(result).isEmpty();
    }

    @Test
    void nonFilterRelNode_traversal() {
        RelNode values = LogicalValues.createOneRow(CLUSTER);
        List<IndexPredicate> result = FilterPredicateExtractor.extract(values);
        assertThat(result).isEmpty();
    }

    @Test
    void comparisonBetweenTwoColumns_producesNoPredicate() {
        RexNode condition = REX.makeCall(SqlStdOperatorTable.EQUALS,
                REX.makeInputRef(ROW_TYPE.getFieldList().get(0).getType(), 0),
                REX.makeInputRef(ROW_TYPE.getFieldList().get(2).getType(), 2));
        List<IndexPredicate> result = extract(condition);

        assertThat(result).isEmpty();
    }

    @Test
    void projectedAliasIsResolvedByInputOrdinalEvenWhenItShadowsAnotherSourceColumn() {
        var projected = org.apache.calcite.rel.logical.LogicalProject.create(SCAN, List.of(),
                List.of(REX.makeInputRef(SCAN, 1)), List.of("status"));
        var condition = REX.makeCall(SqlStdOperatorTable.EQUALS, REX.makeInputRef(projected, 0),
                REX.makeLiteral("ACTIVE"));
        assertThat(FilterPredicateExtractor.extract(LogicalFilter.create(projected, condition)))
                .containsExactly(new EqualTo("name", "ACTIVE"));
    }

    @Test
    void aComputedAliasAndAFilterAboveLimitCannotPruneTheInputScan() {
        var projected = org.apache.calcite.rel.logical.LogicalProject.create(SCAN, List.of(),
                List.of(REX.makeCall(SqlStdOperatorTable.PLUS,
                        REX.makeInputRef(SCAN, 0), REX.makeBigintLiteral(BigDecimal.ONE))), List.of("user_id"));
        var condition = REX.makeCall(SqlStdOperatorTable.EQUALS, REX.makeInputRef(projected, 0),
                REX.makeBigintLiteral(BigDecimal.ONE));
        assertThat(FilterPredicateExtractor.extract(LogicalFilter.create(projected, condition))).isEmpty();
        var limit = org.apache.calcite.rel.logical.LogicalSort.create(SCAN,
                org.apache.calcite.rel.RelCollations.EMPTY, null, REX.makeExactLiteral(BigDecimal.ONE));
        assertThat(FilterPredicateExtractor.extract(LogicalFilter.create(limit, condition))).isEmpty();
        assertThat(FilterPredicateExtractor.extract(org.apache.calcite.rel.logical.LogicalSort.create(
                LogicalFilter.create(SCAN, condition), org.apache.calcite.rel.RelCollations.EMPTY, null,
                REX.makeExactLiteral(BigDecimal.ONE)))).containsExactly(new EqualTo("user_id", 1L));
    }

    @Test
    void inListWithDynamicMemberCannotUseItsLiteralSubsetForPruning() {
        var reference = REX.makeInputRef(SCAN, 0);
        var dynamic = REX.makeDynamicParam(reference.getType(), 0);
        assertThat(extract(REX.makeIn(reference,
                List.of(REX.makeBigintLiteral(BigDecimal.ONE), dynamic)))).isEmpty();
    }

    // --- Helpers ---

    /** Wraps condition in Filter(SCAN) and extracts predicates. */
    private List<IndexPredicate> extract(RexNode condition) {
        return extractWith(condition, SCAN);
    }

    /** Wraps condition in Filter(scan) and extracts predicates. */
    private List<IndexPredicate> extractWith(RexNode condition, LogicalTableScan scan) {
        LogicalFilter filter = LogicalFilter.create(scan, condition);
        return FilterPredicateExtractor.extract(filter);
    }

    private static SqlFunction customFunction(String name) {
        return new SqlFunction(
                name,
                SqlKind.OTHER_FUNCTION,
                ReturnTypes.BOOLEAN_NULLABLE,
                null,
                OperandTypes.VARIADIC,
                SqlFunctionCategory.USER_DEFINED_FUNCTION);
    }

    /**
     * Minimal {@link RelOptTable} for constructing {@link LogicalTableScan}
     * without external dependencies or mock frameworks.
     */
    private static final class MinimalTable implements RelOptTable {
        private final String tableName;
        private final RelDataType rowType;
        MinimalTable(String tableName, RelDataType rowType) {
            this.tableName = tableName;
            this.rowType = rowType;
        }
        @Override public List<String> getQualifiedName() { return List.of(tableName); }
        @Override public RelDataType getRowType() { return rowType; }
        @Override public double getRowCount() { return 100.0; }
        @Override public RelOptSchema getRelOptSchema() { return null; }
        @Override public RelNode toRel(ToRelContext context) { return null; }
        @Override public List<RelCollation> getCollationList() { return List.of(); }
        @Override public RelDistribution getDistribution() { return RelDistributions.ANY; }
        @Override public boolean isKey(ImmutableBitSet columns) { return false; }
        @Override public List<ImmutableBitSet> getKeys() { return List.of(); }
        @Override public List<RelReferentialConstraint> getReferentialConstraints() { return List.of(); }
        @Override public Expression getExpression(Class clazz) { return null; }
        @Override public RelOptTable extend(List<RelDataTypeField> extendedFields) { return this; }
        @Override public List<ColumnStrategy> getColumnStrategies() { return List.of(); }
        @Override public <C> C unwrap(Class<C> clazz) { return null; }
    }
}

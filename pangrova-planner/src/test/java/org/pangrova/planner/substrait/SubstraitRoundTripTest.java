package org.pangrova.planner.substrait;

import org.pangrova.protocol.substrait.SubstraitProtocolVersion;

import io.substrait.plan.Plan;
import org.pangrova.planner.analysis.PangrovaPlanner;
import org.pangrova.planner.analysis.PlanResult;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Substrait IR round-trip consistency test framework (G-IR-07).
 *
 * <p>Verifies that Calcite RelNode plans survive the Substrait conversion pipeline
 * with key structural properties preserved:
 * <pre>
 *   RelNode  --(SubstraitConverter)-->  Plan
 *            --(serialize)-->           byte[]
 *            --(parse proto)-->         proto.Plan
 *            --(verify)-->             structural assertions
 * </pre>
 *
 * <p>Covers: simple SELECT with filter, INNER JOIN, LEFT JOIN, GROUP BY
 * with aggregation, subquery (decorrelated), and UNION ALL.
 *
 * <p>Literal queries additionally verify typed rows through the generated
 * protobuf parser and reparser, without a second private plan decoder.</p>
 */
class SubstraitRoundTripTest {

    private PangrovaPlanner planner;
    private SubstraitConverter converter;
    private SubstraitPlanSerializer serializer;

    @BeforeEach
    void setUp() {
        planner = new PangrovaPlanner(buildTestSchema());
        converter = new SubstraitConverter();
        serializer = new SubstraitPlanSerializer();
    }

    // -----------------------------------------------------------------------
    // 1. Simple SELECT with filter
    // -----------------------------------------------------------------------

    @Test
    void selectWithFilterPreservesTableRefAndFilter() {
        String sql = "SELECT * FROM orders WHERE o_orderkey > 100";
        ProtoSummary summary = roundTripToProto(sql);

        assertThat(summary.relTypes)
                .as("Operator types should include READ")
                .contains("READ");
        assertThat(summary.tableNames)
                .as("Table reference must be preserved")
                .contains("orders");
        assertThat(summary.hasFilterInRead || summary.hasStandaloneFilter)
                .as("Filter condition must be present (in ReadRel or as standalone FilterRel)")
                .isTrue();
        assertThat(summary.rootNames)
                .as("Output column names must be preserved")
                .isNotEmpty();

        verifyByteLevelStability(sql);
    }

    // -----------------------------------------------------------------------
    // 2. INNER JOIN
    // -----------------------------------------------------------------------

    @Test
    void innerJoinPreservesJoinTypeAndTableRefs() {
        String sql = "SELECT * FROM orders o "
                + "JOIN customer c ON o.o_custkey = c.c_custkey";
        ProtoSummary summary = roundTripToProto(sql);

        assertThat(summary.relTypes)
                .as("Operator types must include JOIN")
                .contains("JOIN");
        assertThat(summary.tableNames)
                .as("Both table references must be preserved")
                .contains("orders", "customer");
        assertThat(summary.joinTypes)
                .as("Join type must be INNER")
                .contains("JOIN_TYPE_INNER");
        assertThat(summary.rootNames)
                .as("Output names must be present")
                .isNotEmpty();

        verifyByteLevelStability(sql);
    }

    // -----------------------------------------------------------------------
    // 3. LEFT JOIN
    // -----------------------------------------------------------------------

    @Test
    void leftJoinPreservesJoinType() {
        String sql = "SELECT * FROM orders o "
                + "LEFT JOIN customer c ON o.o_custkey = c.c_custkey";
        ProtoSummary summary = roundTripToProto(sql);

        assertThat(summary.relTypes)
                .as("Operator types must include JOIN")
                .contains("JOIN");
        assertThat(summary.tableNames)
                .as("Both table references must be preserved")
                .contains("orders", "customer");
        assertThat(summary.joinTypes)
                .as("Join type must be LEFT")
                .contains("JOIN_TYPE_LEFT");

        verifyByteLevelStability(sql);
    }

    // -----------------------------------------------------------------------
    // 4. GROUP BY with aggregation
    // -----------------------------------------------------------------------

    @Test
    void groupByPreservesAggregation() {
        String sql = "SELECT o_orderstatus, COUNT(*), SUM(o_totalprice) "
                + "FROM orders GROUP BY o_orderstatus";
        ProtoSummary summary = roundTripToProto(sql);

        assertThat(summary.relTypes)
                .as("Operator types must include AGGREGATE")
                .contains("AGGREGATE");
        assertThat(summary.tableNames)
                .as("Table reference must be preserved")
                .contains("orders");
        assertThat(summary.aggregateMeasureCount)
                .as("Should have 2 aggregate measures (COUNT, SUM)")
                .isEqualTo(2);
        assertThat(summary.rootNames)
                .as("Output column names must include the grouping column alias")
                .isNotEmpty();

        verifyByteLevelStability(sql);
    }

    // -----------------------------------------------------------------------
    // 5. Subquery (IN -> decorrelated to semi/inner join)
    // -----------------------------------------------------------------------

    @Test
    void subqueryProducesValidPlanWithBothTables() {
        String sql = "SELECT * FROM orders "
                + "WHERE o_custkey IN (SELECT c_custkey FROM customer)";
        ProtoSummary summary = roundTripToProto(sql);

        // Calcite decorrelation converts IN-subquery to a join
        assertThat(summary.tableNames)
                .as("Both tables from main query and subquery must be present")
                .contains("orders", "customer");
        // The decorrelated plan should contain a JOIN or CROSS (semi-join path)
        assertThat(summary.relTypes)
                .as("Decorrelated subquery should produce JOIN or AGGREGATE operators")
                .containsAnyOf("JOIN", "CROSS", "AGGREGATE");

        verifyByteLevelStability(sql);
    }

    @Test
    void correlatedExistsRetainsItsEqualityInTheSemiJoin() {
        String sql = "SELECT o_orderkey FROM orders o WHERE EXISTS ("
                + "SELECT * FROM lineitem l WHERE l.l_orderkey = o.o_orderkey "
                + "AND l.l_quantity > 0)";
        ProtoSummary summary = roundTripToProto(sql);

        assertThat(summary.joinTypes).contains("JOIN_TYPE_LEFT_SEMI");
        assertThat(summary.hasJoinCondition).isTrue();
        assertThat(summary.tableNames).contains("orders", "lineitem");
    }

    @Test
    void correlatedNotExistsKeepsNullableMarkerAcrossLeftJoin() {
        String sql = "SELECT c_custkey FROM customer c WHERE NOT EXISTS ("
                + "SELECT * FROM orders o WHERE o.o_custkey = c.c_custkey)";
        ProtoSummary summary = roundTripToProto(sql);

        assertThat(summary.joinTypes).contains("JOIN_TYPE_LEFT");
        assertThat(summary.hasJoinCondition).isTrue();
        assertThat(summary.tableNames).contains("customer", "orders");
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "SELECT o_orderkey FROM orders o WHERE EXISTS (SELECT 1 FROM lineitem l "
                    + "WHERE l.l_orderkey = 1 AND l.l_orderkey = o.o_orderkey)",
            "SELECT o_orderkey FROM orders o WHERE NOT EXISTS (SELECT 1 FROM lineitem l "
                    + "WHERE l.l_orderkey = 1 AND l.l_orderkey = o.o_orderkey)",
            "SELECT o_orderkey FROM orders o WHERE EXISTS (SELECT 1 FROM lineitem l "
                    + "WHERE l.l_orderkey + 1 = o.o_orderkey)",
            "SELECT (SELECT COUNT(*) FROM lineitem l WHERE l.l_orderkey = 1 "
                    + "AND l.l_orderkey = o.o_orderkey) AS n FROM orders o"
    })
    void correlatedConstantAndExpressionKeysRetainTheirJoinPredicate(String sql) {
        ProtoSummary summary = roundTripToProto(sql);
        assertThat(summary.tableNames).contains("orders", "lineitem");
        assertThat(summary.hasJoinCondition).isTrue();
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "SELECT o_orderkey FROM orders o WHERE EXISTS (SELECT 1 FROM lineitem l "
                    + "WHERE CAST(NULL AS BIGINT) + 1 = o.o_orderkey)",
            "SELECT o_orderkey FROM orders o WHERE NOT EXISTS (SELECT 1 FROM lineitem l "
                    + "WHERE CAST(NULL AS BIGINT) + 1 = o.o_orderkey)",
            "SELECT (SELECT COUNT(*) FROM lineitem l WHERE CAST(NULL AS BIGINT) + 1 = o.o_orderkey) AS n "
                    + "FROM orders o",
            "SELECT (SELECT SUM(l_quantity) FROM lineitem l WHERE CAST(NULL AS BIGINT) + 1 = o.o_orderkey) AS n "
                    + "FROM orders o"
    })
    void reducedNullPredicateDoesNotRetainAnUnboundOuterVariable(String sql) {
        ProtoSummary summary = roundTripToProto(sql);
        assertThat(summary.tableNames).contains("orders");
        assertThat(summary.rootNames).isNotEmpty();
    }

    // -----------------------------------------------------------------------
    // 6. UNION ALL
    // -----------------------------------------------------------------------

    @Test
    void unionAllPreservesSetOperation() {
        String sql = "SELECT o_orderkey FROM orders "
                + "UNION ALL SELECT c_custkey FROM customer";
        ProtoSummary summary = roundTripToProto(sql);

        assertThat(summary.relTypes)
                .as("Operator types must include SET for UNION ALL")
                .contains("SET");
        assertThat(summary.tableNames)
                .as("Both tables must be present in the union branches")
                .contains("orders", "customer");
        assertThat(summary.setOpTypes)
                .as("Set operation type should be UNION ALL")
                .contains("SET_OP_UNION_ALL");
        assertThat(summary.rootNames)
                .as("Output column names must be present")
                .isNotEmpty();

        verifyByteLevelStability(sql);
    }

    // -----------------------------------------------------------------------
    // 7. Canonical protobuf round-trip for literal queries
    // -----------------------------------------------------------------------

    @Test
    void planLevelRoundTripPreservesRootNamesAndInputType() throws Exception {
        String sql = "SELECT 42 AS answer, 'hello' AS greeting";
        Plan original = planAndConvert(sql);
        byte[] bytes = serializer.serialize(original);
        var parsed = io.substrait.proto.Plan.parseFrom(bytes);
        var restored = io.substrait.proto.Plan.parseFrom(parsed.toByteArray());

        assertThat(restored.getRelationsList()).hasSameSizeAs(original.getRoots());
        assertThat(restored.getRelations(0).getRoot().getNamesList())
                .as("Root output names must survive full round-trip")
                .isEqualTo(original.getRoots().get(0).getNames());

        var originalInput = parsed.getRelations(0).getRoot().getInput();
        var restoredInput = restored.getRelations(0).getRoot().getInput();
        assertThat(restoredInput.getRelTypeCase())
                .as("Top-level Rel type must be preserved")
                .isEqualTo(originalInput.getRelTypeCase());
        assertThat(restoredInput).isEqualTo(originalInput);
        var read = literalRead(restoredInput);
        assertThat(read.hasBaseSchema()).isTrue();
        assertThat(read.getBaseSchema().getStruct().getTypesCount()).isEqualTo(2);
        assertThat(read.getVirtualTable().getExpressionsCount()).isOne();
        var row = read.getVirtualTable().getExpressions(0);
        assertThat(row.getFieldsCount()).isEqualTo(2);
        assertThat(read.getBaseSchema().getStruct().getTypes(0).hasI32()).isTrue();
        assertThat(row.getFields(0).getLiteral().getI32()).isEqualTo(42);
        assertThat(row.getFields(1).getLiteral().hasString()).isTrue();
        assertThat(row.getFields(1).getLiteral().getString()).isEqualTo("hello");
    }

    @Test
    void variantPathRetainsTheDeclaredNativeSyntax() throws Exception {
        var plan = planAndConvert("SELECT variant_get(o_orderstatus, 'name') FROM orders");
        var proto = io.substrait.proto.Plan.parseFrom(serializer.serialize(plan));
        var project = proto.getRelations(0).getRoot().getInput().getProject();
        var function = project.getExpressions(0).getScalarFunction();
        assertThat(function.getArguments(1).getValue().getLiteral().getString())
                .isEqualTo("name");
    }

    @Test
    void planLevelRoundTripPreservesMultipleExpressions() throws Exception {
        String sql = "SELECT 1, 2, 3";
        Plan original = planAndConvert(sql);
        byte[] bytes = serializer.serialize(original);
        var parsed = io.substrait.proto.Plan.parseFrom(bytes);
        var restored = io.substrait.proto.Plan.parseFrom(parsed.toByteArray());

        assertThat(restored.getRelations(0).getRoot().getNamesList())
                .as("All output names must survive round-trip")
                .hasSameSizeAs(original.getRoots().get(0).getNames());
        assertThat(restored).isEqualTo(parsed);
        var read = literalRead(restored.getRelations(0).getRoot().getInput());
        assertThat(read.getBaseSchema().getStruct().getTypesCount()).isEqualTo(3);
        assertThat(read.getVirtualTable().getExpressionsCount()).isOne();
        var row = read.getVirtualTable().getExpressions(0);
        assertThat(row.getFieldsCount()).isEqualTo(3);
        for (int column = 0; column < 3; column++) {
            assertThat(read.getBaseSchema().getStruct().getTypes(column).hasI32()).isTrue();
            assertThat(row.getFields(column).getLiteral().hasI32()).isTrue();
            assertThat(row.getFields(column).getLiteral().getI32()).isEqualTo(column + 1);
        }
    }

    @Test
    void projectionEmitMappingSurvivesGeneratedProtoRoundTrip() throws Exception {
        Plan original = planAndConvert("SELECT o_totalprice, o_orderkey FROM orders");
        var parsed = io.substrait.proto.Plan.parseFrom(serializer.serialize(original));
        var restored = io.substrait.proto.Plan.parseFrom(parsed.toByteArray());
        assertThat(restored).isEqualTo(parsed);
        var input = restored.getRelations(0).getRoot().getInput();
        assertThat(input.hasProject()).isTrue();
        var project = input.getProject();
        assertThat(project.getExpressionsCount()).isEqualTo(2);
        assertThat(project.getInput().hasRead()).isTrue();
        var schema = project.getInput().getRead().getBaseSchema();
        int inputFields = schema.getStruct().getTypesCount();
        assertThat(project.getCommon().hasEmit()).isTrue();
        assertThat(project.getCommon().getEmit().getOutputMappingList())
                .containsExactly(inputFields, inputFields + 1);
        int firstField = project.getExpressions(0).getSelection()
                .getDirectReference().getStructField().getField();
        int secondField = project.getExpressions(1).getSelection()
                .getDirectReference().getStructField().getField();
        assertThat(schema.getNames(firstField)).isEqualTo("o_totalprice");
        assertThat(schema.getNames(secondField)).isEqualTo("o_orderkey");
    }

    // -----------------------------------------------------------------------
    // 8. Proto-level version stamps survive round-trip
    // -----------------------------------------------------------------------

    @Test
    void versionAndProducerSurviveRoundTrip() throws Exception {
        Plan plan = planAndConvert("SELECT 1");
        byte[] bytes = serializer.serialize(plan);
        io.substrait.proto.Plan proto = io.substrait.proto.Plan.parseFrom(bytes);

        assertThat(proto.hasVersion())
                .as("Serialized plan must carry a Version sub-message")
                .isTrue();
        assertThat(proto.getVersion().getProducer())
                .as("Producer must be stamped")
                .isNotEmpty();
        assertThat(proto.getVersion().getMajorNumber())
                .as("Major version must match the current spec")
                .isEqualTo(SubstraitProtocolVersion.SPEC_MAJOR);
    }

    // -----------------------------------------------------------------------
    // 9. Output column count consistency
    // -----------------------------------------------------------------------

    @Test
    void outputColumnCountConsistentAcrossRoundTrip() {
        String[] queries = {
            "SELECT * FROM orders",
            "SELECT o_orderkey, o_custkey FROM orders",
            "SELECT COUNT(*) FROM orders",
            "SELECT * FROM orders o JOIN customer c ON o.o_custkey = c.c_custkey"
        };
        for (String sql : queries) {
            PlanResult planResult = planner.plan(sql);
            int relNodeFieldCount = planResult.plan().getRowType().getFieldCount();
            Plan plan = converter.convert(planResult.plan(),
                    planResult.preOptimizationColumnNames());
            List<String> rootNames = plan.getRoots().get(0).getNames();

            assertThat(rootNames)
                    .as("Root name count must equal RelNode field count for: " + sql)
                    .hasSize(relNodeFieldCount);
        }
    }

    // ===== Helpers =====

    private static io.substrait.proto.ReadRel literalRead(io.substrait.proto.Rel relation) {
        return switch (relation.getRelTypeCase()) {
            case READ -> relation.getRead();
            case PROJECT -> literalRead(relation.getProject().getInput());
            case FETCH -> literalRead(relation.getFetch().getInput());
            case FILTER -> literalRead(relation.getFilter().getInput());
            default -> throw new AssertionError("Expected literal ReadRel, not " + relation.getRelTypeCase());
        };
    }

    /**
     * Converts a SQL query through the full pipeline and returns a proto-level summary.
     */
    private ProtoSummary roundTripToProto(String sql) {
        Plan plan = planAndConvert(sql);
        byte[] bytes = serializer.serialize(plan);
        try {
            io.substrait.proto.Plan proto = io.substrait.proto.Plan.parseFrom(bytes);
            return summarizeProto(proto);
        } catch (Exception e) {
            throw new AssertionError("Failed to parse serialized proto for: " + sql, e);
        }
    }

    private Plan planAndConvert(String sql) {
        PlanResult planResult = planner.plan(sql);
        return converter.convert(planResult.plan(),
                planResult.preOptimizationColumnNames());
    }

    /**
     * Verifies byte-level stability: serialize -> parse -> re-serialize produces
     * identical bytes. This ensures no information is lost at the proto boundary.
     */
    private void verifyByteLevelStability(String sql) {
        Plan plan = planAndConvert(sql);
        byte[] bytes1 = serializer.serialize(plan);
        try {
            io.substrait.proto.Plan proto = io.substrait.proto.Plan.parseFrom(bytes1);
            byte[] bytes2 = proto.toByteArray();
            assertThat(bytes2)
                    .as("Proto round-trip must produce identical bytes for: " + sql)
                    .isEqualTo(bytes1);
        } catch (Exception e) {
            throw new AssertionError("Byte-level stability check failed for: " + sql, e);
        }
    }

    /**
     * Walks the proto Plan tree and extracts a structural summary for assertions.
     */
    private static ProtoSummary summarizeProto(io.substrait.proto.Plan proto) {
        ProtoSummary summary = new ProtoSummary();
        if (!proto.getRelationsList().isEmpty()) {
            io.substrait.proto.PlanRel planRel = proto.getRelations(0);
            if (planRel.hasRoot()) {
                summary.rootNames = planRel.getRoot().getNamesList();
                walkRel(planRel.getRoot().getInput(), summary);
            }
        }
        return summary;
    }

    /**
     * Recursively walks a proto Rel tree, collecting operator types, table names,
     * join types, aggregate info, and set operation types.
     */
    private static void walkRel(io.substrait.proto.Rel rel, ProtoSummary summary) {
        String relType = rel.getRelTypeCase().name();
        summary.relTypes.add(relType);

        switch (rel.getRelTypeCase()) {
            case READ -> {
                io.substrait.proto.ReadRel read = rel.getRead();
                if (read.getReadTypeCase()
                        == io.substrait.proto.ReadRel.ReadTypeCase.NAMED_TABLE) {
                    List<String> names = read.getNamedTable().getNamesList();
                    // Use the last name segment (table name without catalog/schema)
                    if (!names.isEmpty()) {
                        summary.tableNames.add(names.get(names.size() - 1));
                    }
                }
                if (read.hasFilter()) {
                    summary.hasFilterInRead = true;
                }
            }
            case FILTER -> {
                io.substrait.proto.FilterRel filter = rel.getFilter();
                summary.hasStandaloneFilter = true;
                walkRel(filter.getInput(), summary);
            }
            case PROJECT -> {
                io.substrait.proto.ProjectRel project = rel.getProject();
                walkRel(project.getInput(), summary);
            }
            case JOIN -> {
                io.substrait.proto.JoinRel join = rel.getJoin();
                summary.joinTypes.add(join.getType().name());
                summary.hasJoinCondition |= join.hasExpression();
                walkRel(join.getLeft(), summary);
                walkRel(join.getRight(), summary);
            }
            case CROSS -> {
                io.substrait.proto.CrossRel cross = rel.getCross();
                walkRel(cross.getLeft(), summary);
                walkRel(cross.getRight(), summary);
            }
            case SORT -> {
                io.substrait.proto.SortRel sort = rel.getSort();
                walkRel(sort.getInput(), summary);
            }
            case FETCH -> {
                io.substrait.proto.FetchRel fetch = rel.getFetch();
                walkRel(fetch.getInput(), summary);
            }
            case AGGREGATE -> {
                io.substrait.proto.AggregateRel agg = rel.getAggregate();
                summary.aggregateMeasureCount += agg.getMeasuresCount();
                walkRel(agg.getInput(), summary);
            }
            case SET -> {
                io.substrait.proto.SetRel set = rel.getSet();
                summary.setOpTypes.add(set.getOp().name());
                for (io.substrait.proto.Rel input : set.getInputsList()) {
                    walkRel(input, summary);
                }
            }
            default -> {
                // Extension rels or unknown types -- skip children
            }
        }
    }

    /**
     * Structural summary extracted from a proto Plan for assertion purposes.
     */
    static class ProtoSummary {
        final List<String> relTypes = new ArrayList<>();
        final List<String> tableNames = new ArrayList<>();
        final List<String> joinTypes = new ArrayList<>();
        final List<String> setOpTypes = new ArrayList<>();
        List<String> rootNames = List.of();
        boolean hasFilterInRead;
        boolean hasStandaloneFilter;
        boolean hasJoinCondition;
        int aggregateMeasureCount;
    }

    // ===== Test schema =====

    private static CalciteSchema buildTestSchema() {
        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        schema.add("orders", ordersTable());
        schema.add("customer", customerTable());
        schema.add("lineitem", lineitemTable());
        return schema;
    }

    private static Table ordersTable() {
        return new AbstractTable() {
            @Override
            public RelDataType getRowType(RelDataTypeFactory tf) {
                return tf.builder()
                        .add("o_orderkey", SqlTypeName.BIGINT)
                        .add("o_custkey", SqlTypeName.BIGINT)
                        .add("o_orderstatus", SqlTypeName.VARCHAR, 1)
                        .add("o_totalprice", SqlTypeName.DECIMAL, 15, 2)
                        .add("o_orderdate", SqlTypeName.DATE)
                        .build();
            }
        };
    }

    private static Table customerTable() {
        return new AbstractTable() {
            @Override
            public RelDataType getRowType(RelDataTypeFactory tf) {
                return tf.builder()
                        .add("c_custkey", SqlTypeName.BIGINT)
                        .add("c_name", SqlTypeName.VARCHAR, 25)
                        .add("c_nationkey", SqlTypeName.BIGINT)
                        .add("c_acctbal", SqlTypeName.DECIMAL, 15, 2)
                        .build();
            }
        };
    }

    private static Table lineitemTable() {
        return new AbstractTable() {
            @Override
            public RelDataType getRowType(RelDataTypeFactory tf) {
                return tf.builder()
                        .add("l_orderkey", SqlTypeName.BIGINT)
                        .add("l_partkey", SqlTypeName.BIGINT)
                        .add("l_quantity", SqlTypeName.DECIMAL, 15, 2)
                        .add("l_extendedprice", SqlTypeName.DECIMAL, 15, 2)
                        .build();
            }
        };
    }
}

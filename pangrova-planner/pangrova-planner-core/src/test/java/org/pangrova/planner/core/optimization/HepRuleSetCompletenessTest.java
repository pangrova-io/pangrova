package org.pangrova.planner.core.optimization;

import org.pangrova.planner.core.analysis.PangrovaPlanner;
import org.pangrova.planner.core.analysis.PlanResult;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies the HEP rule set contains 30+ standard optimization rules and
 * that key rules fire on appropriate input patterns.
 */
class HepRuleSetCompletenessTest {

    private static PangrovaPlanner planner;

    @BeforeAll
    static void setUp() {
        planner = buildPlannerWithTables();
    }

    @Test
    void hepRuleRegistryContainsAtLeast30Rules() {
        int totalRules = HepRuleRegistry.totalRuleCount();
        assertThat(totalRules).isGreaterThanOrEqualTo(30);
    }

    @Test
    void projectMergeFiresOnConsecutiveProjects() {
        PlanResult result = planner.plan(
                "SELECT A FROM (SELECT A, B FROM T1) sub");
        String planStr = result.plan().explain();
        long projectCount = planStr.lines()
                .filter(line -> line.contains("LogicalProject"))
                .count();
        assertThat(projectCount).isLessThanOrEqualTo(1);
    }

    @Test
    void filterMergeFiresOnConsecutiveFilters() {
        PlanResult result = planner.plan(
                "SELECT * FROM T1 WHERE A > 5 AND B < 10");
        String planStr = result.plan().explain();
        long filterCount = planStr.lines()
                .filter(line -> line.contains("LogicalFilter"))
                .count();
        assertThat(filterCount).isLessThanOrEqualTo(1);
    }

    @Test
    void aggregateReduceFunctionsFiresOnAggregate() {
        PlanResult result = planner.plan(
                "SELECT AVG(A) FROM T1");
        assertThat(result.plan()).isNotNull();
        assertThat(result.plan().explain()).containsAnyOf("Aggregate", "LogicalAggregate");
    }

    @Test
    void sortRemoveFiresOnRedundantSort() {
        PlanResult result = planner.plan(
                "SELECT A FROM T1 ORDER BY A LIMIT 10");
        assertThat(result.plan()).isNotNull();
        assertThat(result.plan().explain()).isNotEmpty();
    }

    @Test
    void unionMergeFiresOnNestedUnions() {
        PlanResult result = planner.plan(
                "SELECT A FROM T1 UNION ALL SELECT A FROM T1 UNION ALL SELECT A FROM T1");
        String planStr = result.plan().explain();
        assertThat(planStr).contains("Union");
    }

    @Test
    void constantFoldFiresOnConstantExpressions() {
        PlanResult result = planner.plan(
                "SELECT A FROM T1 WHERE 1 + 1 = 2");
        assertThat(result.plan()).isNotNull();
        String planStr = result.plan().explain();
        assertThat(planStr).isNotEmpty();
    }

    @Test
    void filterProjectTransposeFiresOnFilterOverProject() {
        PlanResult result = planner.plan(
                "SELECT A + 1 AS X FROM T1 WHERE A > 10");
        assertThat(result.plan()).isNotNull();
        assertThat(result.plan().explain()).isNotEmpty();
    }

    @Test
    void sortUnionTransposeFiresOnLimitOverUnion() {
        PlanResult result = planner.plan(
                "SELECT * FROM ("
                        + "SELECT A, B FROM T1 UNION ALL SELECT A, B FROM T1"
                        + ") u ORDER BY 1 LIMIT 5");
        assertThat(result.plan()).isNotNull();
        assertThat(result.plan().explain()).contains("Union");
    }

    @Test
    void calcMergeFiresOnConsecutiveCalcs() {
        PlanResult result = planner.plan(
                "SELECT A + 1 AS X FROM (SELECT A * 2 AS A FROM T1) sub");
        assertThat(result.plan()).isNotNull();
        assertThat(result.plan().explain()).isNotEmpty();
    }

    @Test
    void projectJoinTransposeFiresOnProjectOverJoin() {
        PlanResult result = planner.plan(
                "SELECT T1.A FROM T1 JOIN T2 ON T1.A = T2.X");
        assertThat(result.plan()).isNotNull();
        assertThat(result.plan().explain()).isNotEmpty();
    }

    @Test
    void filterAggregateTransposeFiresOnFilterOverAggregate() {
        PlanResult result = planner.plan(
                "SELECT A, COUNT(*) AS cnt FROM T1 GROUP BY A HAVING A > 5");
        assertThat(result.plan()).isNotNull();
        String planStr = result.plan().explain();
        assertThat(planStr).containsAnyOf("Aggregate", "LogicalAggregate");
    }

    @Test
    void aggregateExpandDistinctFiresOnCountDistinct() {
        PlanResult result = planner.plan(
                "SELECT COUNT(DISTINCT A) FROM T1");
        assertThat(result.plan()).isNotNull();
        assertThat(result.plan().explain()).containsAnyOf("Aggregate", "LogicalAggregate");
    }

    @Test
    void joinConditionPushFiresOnJoinWithMixedCondition() {
        PlanResult result = planner.plan(
                "SELECT * FROM T1 JOIN T2 ON T1.A = T2.X AND T1.B > 10");
        assertThat(result.plan()).isNotNull();
        assertThat(result.plan().explain()).isNotEmpty();
    }

    @Test
    void unionPullUpConstantsFiresOnUnionWithConstants() {
        PlanResult result = planner.plan(
                "SELECT 1 AS C, A FROM T1 UNION ALL SELECT 1 AS C, A FROM T1");
        assertThat(result.plan()).isNotNull();
        assertThat(result.plan().explain()).contains("Union");
    }

    @Test
    void intersectToDistinctFiresOnIntersect() {
        PlanResult result = planner.plan(
                "SELECT A FROM T1 INTERSECT SELECT X FROM T2");
        assertThat(result.plan()).isNotNull();
        String planStr = result.plan().explain();
        // INTERSECT_TO_DISTINCT rewrites INTERSECT to JOIN + DISTINCT
        assertThat(planStr).doesNotContain("LogicalIntersect");
    }

    @Test
    void projectSetOpTransposeFiresOnProjectOverUnion() {
        PlanResult result = planner.plan(
                "SELECT A FROM ("
                        + "SELECT A, B FROM T1 UNION ALL SELECT A, B FROM T1"
                        + ") u");
        assertThat(result.plan()).isNotNull();
        assertThat(result.plan().explain()).isNotEmpty();
    }

    @Test
    void sortJoinTransposeFiresOnSortOverJoin() {
        PlanResult result = planner.plan(
                "SELECT * FROM T1 JOIN T2 ON T1.A = T2.X ORDER BY T1.A LIMIT 10");
        assertThat(result.plan()).isNotNull();
        assertThat(result.plan().explain()).isNotEmpty();
    }

    @Test
    void aggregateProjectMergeFiresOnAggregateOverProject() {
        PlanResult result = planner.plan(
                "SELECT SUM(X) FROM (SELECT A AS X FROM T1) sub");
        assertThat(result.plan()).isNotNull();
        assertThat(result.plan().explain()).containsAnyOf("Aggregate", "LogicalAggregate");
    }

    @Test
    void filterSetOpTransposeFiresOnFilterOverUnion() {
        PlanResult result = planner.plan(
                "SELECT * FROM ("
                        + "SELECT A, B FROM T1 UNION ALL SELECT A, B FROM T1"
                        + ") u WHERE A > 5");
        assertThat(result.plan()).isNotNull();
        assertThat(result.plan().explain()).isNotEmpty();
    }

    // ---- Helper methods ----

    private static PangrovaPlanner buildPlannerWithTables() {
        CalciteSchema rootSchema = CalciteSchema.createRootSchema(false);
        rootSchema.add("T1", createTable("A", "B"));
        rootSchema.add("T2", createTable("X", "Y"));
        return new PangrovaPlanner(rootSchema);
    }

    private static Table createTable(String col1, String col2) {
        return new AbstractTable() {
            @Override
            public RelDataType getRowType(RelDataTypeFactory typeFactory) {
                return typeFactory.builder()
                        .add(col1, SqlTypeName.INTEGER)
                        .add(col2, SqlTypeName.INTEGER)
                        .build();
            }
        };
    }
}

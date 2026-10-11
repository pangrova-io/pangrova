package org.pangrova.planner;

import io.substrait.proto.Plan;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.pangrova.planner.optimization.InvalidFieldRefDetector;

import static org.assertj.core.api.Assertions.assertThat;

/** Real SQL compilation through the sole official Calcite rule program. */
class SoleCalciteOptimizationSqlTest {

    @ParameterizedTest
    @ValueSource(strings = {
            "SELECT SUM(CASE WHEN SUBSTRING(v,1,1) LIKE 'a%' THEN 1 ELSE 0 END) AS matches "
                    + "FROM (SELECT 'abc' AS v UNION ALL SELECT 'def' AS v) AS t",
            "SELECT CASE WHEN SUM(CASE WHEN v LIKE 'a%' THEN 1 ELSE 0 END)>0 "
                    + "THEN SUBSTRING('kept',1,4) ELSE 'missing' END AS matches "
                    + "FROM (SELECT 'abc' AS v UNION ALL SELECT 'def' AS v) AS t",
            "SELECT CASE WHEN v LIKE 'a%' THEN SUBSTRING(v,1,2) ELSE 'other' END AS matches "
                    + "FROM (SELECT 'abc' AS v UNION ALL SELECT 'def' AS v) AS t"
    })
    void caseSubstringLikeAndAggregatesCompileWithoutASecondRuleProgram(String sql) throws Exception {
        var compiler = new SqlCompiler();
        var result = compiler.compileToResult(sql);
        assertThat(InvalidFieldRefDetector.containsInvalidFieldRef(result.plan())).isFalse();
        var first = compiler.convertValidateAndSerialize(result.plan(), result.preOptimizationColumnNames());
        var plan = Plan.parseFrom(first);
        assertThat(plan.getRelationsList()).singleElement().satisfies(relation -> {
            assertThat(relation.hasRoot()).isTrue();
            assertThat(relation.getRoot().getNamesList()).containsExactly("matches");
        });
        assertThat(compiler.compile(sql)).isEqualTo(first);
    }
}

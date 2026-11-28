package org.pangrova.planner.rewrite;

import org.junit.jupiter.api.Test;
import org.pangrova.planner.analysis.SqlCompiler;
import org.pangrova.planner.api.exception.PlannerException;
import org.pangrova.planner.parse.PangrovaParserFactory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RecursiveCteExpanderTest {

    private final PangrovaParserFactory parserFactory = new PangrovaParserFactory();
    private final RecursiveCteExpander expander = new RecursiveCteExpander(parserFactory);

    @Test
    void leavesNonRecursiveSqlUntouched() {
        String sql = "WITH c AS (SELECT 1 AS n) SELECT n FROM c";
        assertThat(expander.expand(sql)).isSameAs(sql);
    }

    @Test
    void expandsSequenceIntoExecutableLevels() {
        String sql = "WITH RECURSIVE seq(n) AS ("
                + " SELECT 1"
                + " UNION ALL"
                + " SELECT n + 1 FROM seq WHERE n < 5"
                + ") SELECT n FROM seq";

        String expanded = expander.expand(sql);

        assertThat(expanded).doesNotContainIgnoringCase("RECURSIVE");
        assertThat(expanded).contains("__pangrova_rc_0");
        assertThat(expanded).contains("__pangrova_rc_" + (RecursiveCteExpander.MAX_ITERATIONS - 1));
        assertThat(expanded).contains("seq (n) AS (SELECT * FROM __pangrova_rc_0");
        assertThat(expanded).endsWith("SELECT n FROM seq");
        assertThat(parserFactory.parseToAst(expanded)).isNotNull();
    }

    @Test
    void replacesEveryWorkingTableReferenceWithThePreviousLevel() {
        String step = "SELECT n + 1 FROM seq WHERE n < 5";
        assertThat(RecursiveCteExpander.replaceIdentifier(step, "seq", "__pangrova_rc_3"))
                .isEqualTo("SELECT n + 1 FROM __pangrova_rc_3 WHERE n < 5");
    }

    @Test
    void doesNotReplaceIdentifiersInsideStringLiterals() {
        String step = "SELECT 'seq' AS label, n FROM seq";
        assertThat(RecursiveCteExpander.replaceIdentifier(step, "seq", "level"))
                .isEqualTo("SELECT 'seq' AS label, n FROM level");
    }

    @Test
    void rejectsUnionDistinctRecursion() {
        String sql = "WITH RECURSIVE seq(n) AS ("
                + " SELECT 1"
                + " UNION"
                + " SELECT n + 1 FROM seq WHERE n < 5"
                + ") SELECT n FROM seq";

        assertThatThrownBy(() -> expander.expand(sql))
                .isInstanceOf(PlannerException.class)
                .hasMessageContaining("UNION ALL");
    }

    @Test
    void rejectsPaginationOfTheRecursiveDefinition() {
        String sql = "WITH RECURSIVE seq(n) AS (SELECT 1 UNION ALL "
                + "SELECT n + 1 FROM seq WHERE n < 5 ORDER BY n LIMIT 2) SELECT n FROM seq";
        assertThatThrownBy(() -> expander.expand(sql))
                .isInstanceOf(PlannerException.class)
                .hasMessageContaining("apply ORDER BY, LIMIT or OFFSET in the query consuming recursive CTE");
    }

    @Test
    void compilesRecursiveCteIntoANativeSubstraitPlan() throws Exception {
        byte[] bytes = new SqlCompiler().compile(
                "WITH RECURSIVE seq(n) AS ("
                        + " SELECT 1 UNION ALL SELECT n + 1 FROM seq WHERE n < 5"
                        + ") SELECT n FROM seq");

        var plan = org.pangrova.protocol.substrait.SubstraitPlanCodec.parse(bytes);
        assertThat(plan.getRelationsCount()).isEqualTo(1);
    }

    @Test
    void keepsLevelCountConfigurable() {
        RecursiveCteExpander small = new RecursiveCteExpander(parserFactory, 3);
        String expanded = small.expand("WITH RECURSIVE seq(n) AS ("
                + " SELECT 1 UNION ALL SELECT n + 1 FROM seq WHERE n < 5"
                + ") SELECT n FROM seq");

        assertThat(expanded).contains("__pangrova_rc_2");
        assertThat(expanded).doesNotContain("__pangrova_rc_3");
    }
}

package org.pangrova.indexing.adapter.planner.vector;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalSort;
import org.junit.jupiter.api.Test;
import org.pangrova.planner.core.analysis.SqlCompiler;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** SQL pattern checks with no index: not a substitute for indexed Native execution. */
class VectorSearchRewriterCastTest {
    @Test
    void decimalQueryCastStillReachesTheIndexLookupForTheSamePhysicalFloatColumn() {
        var lookupColumns = lookupColumns("ARRAY[1.25,2.5]");
        assertThat(lookupColumns).containsExactly("vec");
    }

    @Test
    void wideningTheIndexedFloatColumnToDoubleDoesNotUseTheFloatIndex() {
        var lookupColumns = lookupColumns("ARRAY[CAST(1.25 AS DOUBLE),CAST(2.5 AS DOUBLE)]");
        assertThat(lookupColumns).isEmpty();
    }

    private static List<String> lookupColumns(String queryVector) {
        var compiled = new SqlCompiler().compileForPolicyApplication("SELECT L2_DISTANCE(vec," + queryVector
                + ") FROM (SELECT ARRAY[CAST(1 AS FLOAT),CAST(2 AS FLOAT)] AS vec "
                + "UNION ALL SELECT ARRAY[CAST(3 AS FLOAT),CAST(4 AS FLOAT)]) t ORDER BY 1 LIMIT 5");
        var lookupColumns = new ArrayList<String>();
        var result = VectorSearchRewriter.tryRewrite(findSort(compiled.plan()), (table, column) -> {
            lookupColumns.add(column);
            return List.of();
        });
        assertThat(result).isEmpty();
        return lookupColumns;
    }

    private static LogicalSort findSort(RelNode node) {
        if (node instanceof LogicalSort sort) return sort;
        assertThat(node.getInputs()).hasSize(1);
        return findSort(node.getInput(0));
    }
}

package org.pangrova.planner.rule;

import org.pangrova.parser.ast.CteQuery;
import org.pangrova.parser.ast.Statement;
import org.pangrova.parser.ast.Query;
import org.pangrova.parser.ast.SetQuery;
import org.pangrova.parser.ast.TableRef;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Counts how many times each CTE name is referenced in the query body.
 * Used by the planner to decide whether CTE materialization would be beneficial.
 */
public final class CteReferenceCounter {

    private CteReferenceCounter() {}

    /**
     * Analyzes a CteQuery AST and returns a list of CTE references with their
     * reference counts. Only CTEs defined in the WITH clause are counted.
     *
     * @param cteQuery the parsed CTE query
     * @return list of CTE references with name and reference count
     */
    public static List<CTEReference> count(CteQuery cteQuery) {
        Map<String, Integer> counts = new HashMap<>();
        for (CteQuery.CteDefinition def : cteQuery.ctes()) {
            counts.put(def.name().toLowerCase(), 0);
        }
        countInStatement(cteQuery.body(), counts);
        for (CteQuery.CteDefinition def : cteQuery.ctes()) {
            countInStatement(def.query(), counts);
        }
        return cteQuery.ctes().stream()
                .map(def -> {
                    String name = def.name().toLowerCase();
                    int refCount = counts.getOrDefault(name, 0);
                    return new CTEReference(def.name(), estimateCost(def.query()), refCount);
                })
                .toList();
    }

    private static void countInStatement(Statement stmt, Map<String, Integer> counts) {
        if (stmt instanceof Query query) {
            countInQuery(query, counts);
        } else if (stmt instanceof SetQuery setQuery) {
            for (Query q : setQuery.queries()) {
                countInQuery(q, counts);
            }
        } else if (stmt instanceof CteQuery nested) {
            for (CteQuery.CteDefinition def : nested.ctes()) {
                countInStatement(def.query(), counts);
            }
            countInStatement(nested.body(), counts);
        }
    }

    private static void countInQuery(Query query, Map<String, Integer> counts) {
        for (TableRef ref : query.from()) {
            countInTableRef(ref, counts);
        }
        query.where().ifPresent(expr -> countInExpression(expr, counts));
        query.having().ifPresent(expr -> countInExpression(expr, counts));
    }

    private static void countInTableRef(TableRef ref, Map<String, Integer> counts) {
        if (ref.isDerived()) {
            ref.subquery().ifPresent(sub -> countInStatement(sub, counts));
        } else {
            List<String> parts = ref.name().parts();
            String tableName = parts.get(parts.size() - 1).toLowerCase();
            if (counts.containsKey(tableName)) {
                counts.merge(tableName, 1, Integer::sum);
            }
        }
    }

    private static void countInExpression(
            org.pangrova.parser.ast.Expression expr, Map<String, Integer> counts) {
        if (expr instanceof org.pangrova.parser.ast.expression.SubqueryExpression sub) {
            countInStatement(sub.subquery(), counts);
        } else if (expr instanceof org.pangrova.parser.ast.expression.ExistsExpression exists) {
            countInStatement(exists.subquery(), counts);
        } else if (expr instanceof org.pangrova.parser.ast.expression.InSubqueryExpression inSub) {
            countInStatement(inSub.subquery(), counts);
        }
    }

    /**
     * Rough cost estimate based on query complexity.
     * Uses number of FROM tables as a proxy for computational cost.
     */
    private static double estimateCost(Statement stmt) {
        if (stmt instanceof Query query) {
            return 100.0 * Math.max(1, query.from().size());
        }
        return 100.0;
    }
}

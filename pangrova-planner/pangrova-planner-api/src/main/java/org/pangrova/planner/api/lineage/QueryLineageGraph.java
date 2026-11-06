package org.pangrova.planner.api.lineage;

import org.pangrova.common.api.annotation.Internal;

import java.util.List;
import java.util.Set;

/**
 * Complete lineage graph for a single query execution.
 * Contains all column derivations and the set of source tables accessed.
 *
 * @param queryId     unique query identifier
 * @param sql         original SQL text
 * @param derivations column-level derivation mappings (output → sources)
 * @param sourceTables fully qualified source table names accessed by the query
 * @param targetTables fully qualified target table names written by the query (DML)
 */
@Internal
public record QueryLineageGraph(
        String queryId,
        String sql,
        List<ColumnDerivation> derivations,
        Set<String> sourceTables,
        Set<String> targetTables
) {
    public QueryLineageGraph {
        derivations = derivations == null ? List.of() : List.copyOf(derivations);
        sourceTables = sourceTables == null ? Set.of() : Set.copyOf(sourceTables);
        targetTables = targetTables == null ? Set.of() : Set.copyOf(targetTables);
    }
}

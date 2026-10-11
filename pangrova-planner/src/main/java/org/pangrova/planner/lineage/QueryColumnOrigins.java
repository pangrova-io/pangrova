package org.pangrova.planner.lineage;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Uncollect;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.metadata.JaninoRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexFieldAccess;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/** Query-owned Calcite metadata scope; no plan-bearing provider enters Calcite's static handler cache. */
final class QueryColumnOrigins extends RelMetadataQuery {
    private final PangrovaColumnOriginMetadata scoped;
    private final ColumnLineageKinds kinds;

    QueryColumnOrigins(RelNode root) {
        super(JaninoRelMetadataProvider.of(root.getCluster().getMetadataProvider()));
        var correlations = new HashMap<CorrelationId, RelNode>();
        collect(root, correlations, java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<>()));
        scoped = new PangrovaColumnOriginMetadata(correlations);
        kinds = new ColumnLineageKinds(correlations);
    }

    @Override
    public Set<RelColumnOrigin> getColumnOrigins(RelNode relation, int field) {
        relation = ColumnLineageKinds.unwrap(relation);
        if (relation instanceof Project project) {
            return scoped.getColumnOrigins(project, this, field);
        }
        if (relation instanceof Calc calc) {
            return scoped.getColumnOrigins(calc, this, field);
        }
        if (relation instanceof Window window) {
            return scoped.getColumnOrigins(window, this, field);
        }
        if (relation instanceof Correlate correlate) {
            return scoped.getColumnOrigins(correlate, this, field);
        }
        if (relation instanceof Uncollect uncollect) {
            return scoped.getColumnOrigins(uncollect, this, field);
        }
        return super.getColumnOrigins(relation, field);
    }

    ColumnDerivation.DerivationType classification(RelNode relation, int field) {
        return kinds.field(relation, field);
    }

    static RelNode requireCorrelationOwner(
            Map<CorrelationId, RelNode> correlations,
            RexCorrelVariable variable, RexFieldAccess access) {
        var owner = correlations.get(variable.id);
        if (owner == null) {
            throw new IllegalArgumentException("Column lineage correlation has no matching actual input owner");
        }
        var actual = owner.getRowType().getFieldList();
        var declared = variable.getType().getFieldList();
        int index = access.getField().getIndex();
        if (index < 0 || index >= actual.size() || declared.size() < actual.size()
                || !actual.get(index).getType().equals(access.getField().getType())) {
            throw new IllegalArgumentException("Column lineage correlation has no matching actual input owner");
        }
        for (int field = 0; field < actual.size(); field++) {
            if (!actual.get(field).getName().equals(declared.get(field).getName())
                    || !actual.get(field).getType().equals(declared.get(field).getType())) {
                throw new IllegalArgumentException("Column lineage correlation has no matching actual input owner");
            }
        }
        return owner;
    }

    private static void collect(RelNode relation, Map<CorrelationId, RelNode> correlations, Set<RelNode> visited) {
        relation = ColumnLineageKinds.unwrap(relation);
        if (!visited.add(relation)) {
            return;
        }
        if (relation instanceof Correlate correlate) {
            var left = ColumnLineageKinds.unwrap(correlate.getLeft());
            var previous = correlations.putIfAbsent(correlate.getCorrelationId(), left);
            if (previous != null && previous != left) {
                throw new IllegalArgumentException("Correlation identity has different actual input owners");
            }
        }
        for (var input : relation.getInputs()) {
            collect(input, correlations, visited);
        }
    }
}

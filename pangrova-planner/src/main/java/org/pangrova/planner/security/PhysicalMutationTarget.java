package org.pangrova.planner.security;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;

import java.util.ArrayList;
import java.util.List;

/** Separates match-only optimizer predicates from physical rows required by a replacement write. */
record PhysicalMutationTarget(TableScan scan, RelNode relation, List<RexNode> matchPredicates) {
    PhysicalMutationTarget {
        matchPredicates = List.copyOf(matchPredicates);
    }

    static PhysicalMutationTarget extract(RelNode input) {
        if (input instanceof TableScan scan) {
            return new PhysicalMutationTarget(scan, scan, List.of());
        }
        if (input instanceof LogicalFilter filter) {
            var source = extract(filter.getInput());
            List<RexNode> predicates = new ArrayList<>(source.matchPredicates());
            predicates.add(filter.getCondition());
            return new PhysicalMutationTarget(source.scan(), source.relation(), predicates);
        }
        if (input instanceof LogicalProject project && isIdentity(project)) {
            var source = extract(project.getInput());
            return new PhysicalMutationTarget(source.scan(), project.copy(project.getTraitSet(),
                    source.relation(), project.getProjects(), project.getRowType()), source.matchPredicates());
        }
        throw new IllegalArgumentException("Protected mutation requires an unmodified physical target scan");
    }

    private static boolean isIdentity(LogicalProject project) {
        if (project.getProjects().size() != project.getInput().getRowType().getFieldCount()) {
            return false;
        }
        for (int index = 0; index < project.getProjects().size(); index++) {
            if (!(project.getProjects().get(index) instanceof RexInputRef reference)
                    || reference.getIndex() != index) {
                return false;
            }
        }
        return true;
    }
}

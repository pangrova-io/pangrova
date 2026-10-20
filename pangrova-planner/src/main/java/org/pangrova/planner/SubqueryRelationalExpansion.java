package org.pangrova.planner;

import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.pangrova.planner.optimization.HepRuleRegistry;

/** Exposes subquery inputs as relational children before expansion and authorization. */
final class SubqueryRelationalExpansion {
    private SubqueryRelationalExpansion() {}

    static RelNode expand(RelNode relation) {
        var program = new HepProgramBuilder();
        // These structural rules do not run expression reduction, source pruning or cost optimization.
        HepRuleRegistry.registerSubqueryDecorrelation(program);
        var planner = new HepPlanner(program.build());
        planner.setRoot(relation);
        return planner.findBestExp();
    }
}

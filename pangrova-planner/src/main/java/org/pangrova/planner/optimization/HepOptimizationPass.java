package org.pangrova.planner.optimization;

import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.rel.RelNode;
import org.pangrova.planner.type.PlannerConstantExecutor;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.planner.PlannerException;

import java.util.Objects;

/** Applies the single Calcite HEP program with a fresh planner for each query. */
public final class HepOptimizationPass implements OptimizationPass {

    private final HepProgram hepProgram;

    public HepOptimizationPass(HepProgram hepProgram) {
        this.hepProgram = Objects.requireNonNull(hepProgram, "hepProgram");
    }

    @Override
    public RelNode optimize(RelNode relNode, OptimizationContext context) {
        final RelNode result;
        try {
            HepPlanner planner = new HepPlanner(hepProgram);
            planner.setExecutor(PlannerConstantExecutor.INSTANCE);
            planner.setRoot(relNode);
            result = planner.findBestExp();
        } catch (RuntimeException failure) {
            throw new PlannerException(PangrovaErrorCode.PLANNER_OPTIMIZATION_FAILURE,
                    "Calcite HEP optimization failed", failure);
        }
        if (InvalidFieldRefDetector.containsInvalidFieldRef(result)) {
            throw new PlannerException(PangrovaErrorCode.PLANNER_OPTIMIZATION_FAILURE,
                    "Calcite HEP produced invalid field references");
        }
        return result;
    }

    @Override
    public String name() {
        return "HepRbo";
    }
}

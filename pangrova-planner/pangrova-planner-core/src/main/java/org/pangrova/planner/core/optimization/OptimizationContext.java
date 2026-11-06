package org.pangrova.planner.core.optimization;

import org.pangrova.planner.core.cost.CboConfig;
import org.pangrova.planner.core.cost.CostModelConfig;
import org.pangrova.planner.core.cost.IndexCostFactor;
import org.pangrova.planner.core.cost.StatisticsBasedCostEstimator;

/**
 * Immutable context passed to each {@link OptimizationPass} during pipeline execution.
 * Carries configuration and optional cost estimation infrastructure.
 *
 * @param cboConfig       Cascades CBO search budget configuration
 * @param costModelConfig cost model parameters (CPU, I/O, network weights)
 * @param costEstimator   statistics-based cost estimator, or null when statistics unavailable
 * @param indexCostFactor  index-aware scan cost reducer, or null when no index metadata available
 */
public record OptimizationContext(
        CboConfig cboConfig,
        CostModelConfig costModelConfig,
        StatisticsBasedCostEstimator costEstimator,
        IndexCostFactor indexCostFactor) {

    public OptimizationContext {
        if (cboConfig == null) {
            throw new NullPointerException("cboConfig");
        }
        if (costModelConfig == null) {
            throw new NullPointerException("costModelConfig");
        }
    }

    /**
     * Convenience constructor for callers without index metadata.
     */
    public OptimizationContext(CboConfig cboConfig, CostModelConfig costModelConfig,
                               StatisticsBasedCostEstimator costEstimator) {
        this(cboConfig, costModelConfig, costEstimator, null);
    }
}

package org.pangrova.planner.analysis;

import org.pangrova.planner.cost.CboConfig;
import org.pangrova.planner.cost.CostModelConfig;
import org.pangrova.planner.runtimefilter.DynamicPartitionPruningConfig;
import org.pangrova.planner.runtimefilter.RuntimeFilterRegistry;
import org.pangrova.planner.mv.DynamicTableRewriter;
import org.pangrova.planner.mv.MaterializedViewRewriteCatalog;
import org.pangrova.planner.api.metadata.MetadataVersionProvider;
import org.pangrova.planner.statistics.StatisticsCollector;

/**
 * Encapsulates optional advanced feature dependencies for {@link PangrovaPlanner}.
 * Null fields disable the corresponding feature.
 *
 * @param statisticsCollector statistics collector for CBO, or null to disable
 * @param materializedViewCatalog published materialized view catalog, or null to disable rewrite
 * @param dynamicPartitionPruningConfig DPP configuration, or null to disable
 * @param runtimeFilterRegistry shared registry for DPP runtime filters, or null to create internal
 * @param dynamicTableRewriter Dynamic Table query rewriter, or null to disable DT transparency
 * @param metadataVersionProvider metadata version provider for cross-catalog consistency, or null to skip
 * @param cboConfig Cascades CBO search budget configuration, or null to disable CBO
 * @param costModelConfig cost model parameters for CBO, or null to use SSD defaults
 */
public record PlannerContext(
        StatisticsCollector statisticsCollector,
        MaterializedViewRewriteCatalog materializedViewCatalog,
        DynamicPartitionPruningConfig dynamicPartitionPruningConfig,
        RuntimeFilterRegistry runtimeFilterRegistry,
        DynamicTableRewriter dynamicTableRewriter,
        MetadataVersionProvider metadataVersionProvider,
        CboConfig cboConfig,
        CostModelConfig costModelConfig) {

    /** Empty dependencies — all optional features disabled. */
    public static final PlannerContext NONE = new PlannerContext(
            null, null, null, null, null, null, null, null);

    /** Backward-compatible constructor without CBO config. */
    public PlannerContext(
            StatisticsCollector statisticsCollector,
            MaterializedViewRewriteCatalog materializedViewCatalog,
                DynamicPartitionPruningConfig dynamicPartitionPruningConfig,
            RuntimeFilterRegistry runtimeFilterRegistry,
            DynamicTableRewriter dynamicTableRewriter,
            MetadataVersionProvider metadataVersionProvider) {
        this(statisticsCollector, materializedViewCatalog,
                dynamicPartitionPruningConfig, runtimeFilterRegistry,
                dynamicTableRewriter, metadataVersionProvider, null, null);
    }
}

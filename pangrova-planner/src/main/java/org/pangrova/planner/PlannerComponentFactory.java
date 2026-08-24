package org.pangrova.planner;

import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.pangrova.planner.cost.StatisticsBasedCostEstimator;
import org.pangrova.planner.optimization.HepRuleRegistry;
import org.pangrova.planner.rule.CteRelOptRule;
import org.pangrova.planner.rule.PartitionPruningRelOptRule;
import org.pangrova.planner.stats.StatisticsCollector;
import org.pangrova.planner.validation.PangrovaSqlOperatorTable;

import java.util.List;
import java.util.Properties;

/**
 * Factory for the Calcite components used per plan() invocation:
 * catalog reader, validator, cluster and the HEP optimization programs.
 */
final class PlannerComponentFactory {

    private PlannerComponentFactory() {
    }

    static CalciteCatalogReader buildCatalogReader(
            RelDataTypeFactory typeFactory,
            CalciteSchema rootSchema, List<String> defaultSchemaPath) {
        Properties props = new Properties();
        props.setProperty("caseSensitive", "false");
        return new CalciteCatalogReader(
                rootSchema,
                defaultSchemaPath,
                typeFactory,
                new CalciteConnectionConfigImpl(props));
    }

    static SqlValidator buildValidator(
            PangrovaSqlOperatorTable operatorTable,
            CalciteCatalogReader catalogReader,
            RelDataTypeFactory typeFactory) {
        return SqlValidatorUtil.newValidator(
                operatorTable,
                catalogReader,
                typeFactory,
                SqlValidator.Config.DEFAULT
                        .withConformance(org.apache.calcite.sql.validate.SqlConformanceEnum.LENIENT)
                        .withIdentifierExpansion(true));
    }

    static RelOptCluster buildCluster(
            RelDataTypeFactory typeFactory,
            StatisticsBasedCostEstimator costEstimator) {
        // PERF-T16: Create VolcanoPlanner with a CancelFlag context to prevent
        // search-space explosion on complex queries (multi-table JOINs + subqueries).
        // The CancelFlag is cooperative — checked by VolcanoPlanner at each rule match.
        VolcanoPlanner planner = new VolcanoPlanner(
                null,
                org.apache.calcite.plan.Contexts.of(
                        new org.apache.calcite.util.CancelFlag(new java.util.concurrent.atomic.AtomicBoolean(false))));
        planner.setExecutor(RexUtil.EXECUTOR);
        RelOptCluster cluster = RelOptCluster.create(planner, new RexBuilder(typeFactory));
        if (costEstimator != null) {
            var selectivityProvider = new org.pangrova.planner.cost.PangrovaRelMdSelectivity(costEstimator);
            cluster.setMetadataProvider(
                    org.apache.calcite.rel.metadata.ChainedRelMetadataProvider.of(
                            List.of(selectivityProvider.asProvider(),
                                    cluster.getMetadataProvider())));
        }
        return cluster;
    }

    static HepProgram buildHepProgram(StatisticsCollector statisticsCollector) {
        HepProgramBuilder builder = new HepProgramBuilder();
        // Register all standard optimization rules via the centralized registry
        HepRuleRegistry.registerAll(builder);
        // CTE materialization: detect duplicate subplans and spool them
        builder.addRuleInstance(CteRelOptRule.INSTANCE);
        // Partition pruning: eliminate partitions outside predicate range
        if (statisticsCollector != null) {
            builder.addRuleInstance(new PartitionPruningRelOptRule(statisticsCollector));
        }
        return builder.build();
    }

    /**
     * Safe HEP program without REDUCE_EXPRESSIONS rules.
     * Used as fallback when expression reduction produces invalid RexInputRef
     * indices (Calcite bug with SUBSTRING/LIKE inside CASE/aggregate).
     */
    static HepProgram buildHepProgramSafe() {
        HepProgramBuilder builder = new HepProgramBuilder();
        HepRuleRegistry.registerAllSafe(builder);
        return builder.build();
    }
}

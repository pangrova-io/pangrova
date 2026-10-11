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
import org.pangrova.planner.cost.CostModelConfig;
import org.pangrova.planner.cost.PangrovaCostFactory;
import org.pangrova.planner.cost.PangrovaRelMdJoinCost;
import org.pangrova.planner.cost.distribution.PangrovaDistributionTraitDef;
import org.pangrova.planner.optimization.HepRuleRegistry;
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
            StatisticsBasedCostEstimator costEstimator,
            CostModelConfig costModelConfig,
            boolean cboEnabled) {
        // PERF-T16: Create VolcanoPlanner with a CancelFlag context to prevent
        // search-space explosion on complex queries (multi-table JOINs + subqueries).
        // The CancelFlag is cooperative — checked by VolcanoPlanner at each rule match.
        VolcanoPlanner planner = new VolcanoPlanner(
                new PangrovaCostFactory(costModelConfig),
                org.apache.calcite.plan.Contexts.of(
                        new org.apache.calcite.util.CancelFlag(new java.util.concurrent.atomic.AtomicBoolean(false))));
        // Traits must exist before SqlToRel creates nodes. Adding them only
        // when CBO registers the root leaves pre-existing trait sets invalid.
        planner.addRelTraitDef(org.apache.calcite.plan.ConventionTraitDef.INSTANCE);
        planner.addRelTraitDef(PangrovaDistributionTraitDef.INSTANCE);
        // The current native-serializable relational operators retain NONE
        // convention; the serializer validates their concrete operator types.
        planner.setNoneConventionHasInfiniteCost(false);
        planner.setExecutor(RexUtil.EXECUTOR);
        RelOptCluster cluster = RelOptCluster.create(planner, new RexBuilder(typeFactory));
        var metadataProviders = new java.util.ArrayList<
                org.apache.calcite.rel.metadata.RelMetadataProvider>();
        metadataProviders.add(new org.pangrova.planner.lineage.PangrovaColumnOriginMetadata().asProvider());
        if (cboEnabled) {
            metadataProviders.add(new PangrovaRelMdJoinCost(costModelConfig).asProvider());
        }
        if (costEstimator != null) {
            metadataProviders.add(new org.pangrova.planner.cost.PangrovaRelMdSelectivity(
                    costEstimator).asProvider());
        }
        if (!metadataProviders.isEmpty()) {
            metadataProviders.add(cluster.getMetadataProvider());
            cluster.setMetadataProvider(
                    org.apache.calcite.rel.metadata.ChainedRelMetadataProvider.of(
                            metadataProviders));
        }
        return cluster;
    }

    static HepProgram buildHepProgram() {
        HepProgramBuilder builder = new HepProgramBuilder();
        // Register all standard optimization rules via the centralized registry
        HepRuleRegistry.registerAll(builder);
        return builder.build();
    }

}

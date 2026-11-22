package org.pangrova.planner.analysis;

import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.metadata.ChainedRelMetadataProvider;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.pangrova.planner.type.PlannerRexBuilder;
import org.pangrova.planner.type.PlannerConstantExecutor;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.pangrova.planner.cost.StatisticsBasedCostEstimator;
import org.pangrova.planner.cost.CostModelConfig;
import org.pangrova.planner.cost.PangrovaCostFactory;
import org.pangrova.planner.cost.PangrovaRelMdJoinCost;
import org.pangrova.planner.cost.PangrovaRelMdSelectivity;
import org.pangrova.planner.lineage.PangrovaColumnOriginMetadata;
import org.pangrova.planner.cost.distribution.PangrovaDistributionTraitDef;
import org.pangrova.planner.optimization.HepRuleRegistry;
import org.pangrova.planner.validation.PangrovaSqlOperatorTable;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Factory for the Calcite components used per plan() invocation:
 * catalog reader, validator, cluster and the HEP optimization programs.
 */
final class PlannerComponentFactory {

    // Calcite's generated handler cache uses the provider chain as its key.
    // The handlers are shared and read query-owned inputs from the relation's planner.
    private static final RelMetadataProvider COLUMN_ORIGINS = new PangrovaColumnOriginMetadata().asProvider();
    private static final RelMetadataProvider LINEAGE = metadataChain();
    private static final RelMetadataProvider WITH_JOIN_COST = metadataChain(PangrovaRelMdJoinCost.provider());
    private static final RelMetadataProvider WITH_SELECTIVITY = metadataChain(PangrovaRelMdSelectivity.provider());
    private static final RelMetadataProvider WITH_COST_AND_SELECTIVITY = metadataChain(
            PangrovaRelMdJoinCost.provider(), PangrovaRelMdSelectivity.provider());

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
        var cancelFlag = new org.apache.calcite.util.CancelFlag(new java.util.concurrent.atomic.AtomicBoolean(false));
        var context = costEstimator == null
                ? Contexts.of(cancelFlag, costModelConfig) : Contexts.of(cancelFlag, costModelConfig, costEstimator);
        VolcanoPlanner planner = new VolcanoPlanner(new PangrovaCostFactory(costModelConfig), context);
        // Traits must exist before SqlToRel creates nodes. Adding them only
        // when CBO registers the root leaves pre-existing trait sets invalid.
        planner.addRelTraitDef(org.apache.calcite.plan.ConventionTraitDef.INSTANCE);
        planner.addRelTraitDef(PangrovaDistributionTraitDef.INSTANCE);
        // The current native-serializable relational operators retain NONE
        // convention; the serializer validates their concrete operator types.
        planner.setNoneConventionHasInfiniteCost(false);
        planner.setExecutor(PlannerConstantExecutor.INSTANCE);
        RelOptCluster cluster = RelOptCluster.create(planner, new PlannerRexBuilder(typeFactory));
        cluster.setMetadataProvider(cboEnabled
                ? (costEstimator == null ? WITH_JOIN_COST : WITH_COST_AND_SELECTIVITY)
                : (costEstimator == null ? LINEAGE : WITH_SELECTIVITY));
        return cluster;
    }

    private static RelMetadataProvider metadataChain(RelMetadataProvider... planningProviders) {
        var providers = new ArrayList<RelMetadataProvider>(planningProviders.length + 2);
        providers.add(COLUMN_ORIGINS);
        providers.addAll(List.of(planningProviders));
        providers.add(DefaultRelMetadataProvider.INSTANCE);
        return ChainedRelMetadataProvider.of(providers);
    }

    static HepProgram buildHepProgram() {
        HepProgramBuilder builder = new HepProgramBuilder();
        // Register all standard optimization rules via the centralized registry
        HepRuleRegistry.registerAll(builder);
        return builder.build();
    }

}

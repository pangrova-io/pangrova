package org.pangrova.planner.cost.distribution;

import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;

/**
 * Trait definition for {@link PangrovaDistribution}.
 * Registered with the Volcano planner to enable automatic Exchange enforcer insertion
 * when a parent operator requires a different distribution than its child provides.
 */
public final class PangrovaDistributionTraitDef extends RelTraitDef<PangrovaDistribution> {

    public static final PangrovaDistributionTraitDef INSTANCE = new PangrovaDistributionTraitDef();

    private PangrovaDistributionTraitDef() {
    }

    @Override
    public Class<PangrovaDistribution> getTraitClass() {
        return PangrovaDistribution.class;
    }

    @Override
    public String getSimpleName() {
        return "pangrova-distribution";
    }

    @Override
    public PangrovaDistribution getDefault() {
        return PangrovaDistribution.ANY;
    }

    @Override
    public RelNode convert(
            RelOptPlanner planner,
            RelNode rel,
            PangrovaDistribution toTrait,
            boolean allowInfiniteCostConverters) {
        PangrovaDistribution currentTrait = rel.getTraitSet().getTrait(this);
        if (currentTrait != null && currentTrait.satisfies(toTrait)) {
            return rel;
        }
        RelTraitSet newTraitSet = rel.getTraitSet().replace(toTrait);
        return new PangrovaExchange(rel.getCluster(), newTraitSet, rel, toTrait);
    }

    @Override
    public boolean canConvert(
            RelOptPlanner planner,
            PangrovaDistribution fromTrait,
            PangrovaDistribution toTrait) {
        return true;
    }
}

package org.pangrova.planner.cost.convention;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;

/**
 * Calcite Convention representing the Pangrova physical execution layer.
 * All physical operators that can be executed by the Pangrova native engine
 * carry this convention in their traitSet, enabling the Volcano planner
 * to distinguish physical (executable) plans from logical (abstract) plans
 * and perform convention-based physical layer selection.
 *
 * <p>Usage: physical operators implement {@link PangrovaRel} and include
 * {@code PangrovaConvention.INSTANCE} in their traitSet.</p>
 */
public final class PangrovaConvention implements Convention {
    public static final PangrovaConvention INSTANCE = new PangrovaConvention();

    private PangrovaConvention() {}

    @Override
    public Class<?> getInterface() {
        return PangrovaRel.class;
    }

    @Override
    public String getName() {
        return "PANGROVA";
    }

    @Override
    public RelTraitDef<Convention> getTraitDef() {
        return ConventionTraitDef.INSTANCE;
    }

    @Override
    public boolean satisfies(RelTrait trait) {
        return trait == this;
    }

    @Override
    public void register(RelOptPlanner planner) {
        // This convention adds no planner rules.
    }

    @Override
    public boolean canConvertConvention(Convention toConvention) {
        return false;
    }

    @Override
    public boolean useAbstractConvertersForConversion(
            RelTraitSet toTraits, RelTraitSet fromTraits) {
        return false;
    }

    @Override
    public String toString() {
        return getName();
    }
}

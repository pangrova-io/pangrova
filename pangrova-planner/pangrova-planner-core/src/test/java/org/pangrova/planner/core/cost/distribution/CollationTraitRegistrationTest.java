package org.pangrova.planner.core.cost.distribution;

import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that the Pangrova CBO pipeline registers collation as a first-class
 * Calcite trait alongside convention and distribution.
 */
class CollationTraitRegistrationTest {

    @Test
    void collationTraitDefRegisteredInVolcanoPlanner() {
        VolcanoPlanner planner = new VolcanoPlanner();
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        planner.addRelTraitDef(PangrovaDistributionTraitDef.INSTANCE);
        PangrovaCollationTraitDef.register(planner);

        // Verify all three traits are registered.
        assertThat(planner.getRelTraitDefs())
                .as("All three traits (convention, distribution, collation) must be registered")
                .contains(ConventionTraitDef.INSTANCE)
                .contains(PangrovaDistributionTraitDef.INSTANCE)
                .contains(RelCollationTraitDef.INSTANCE);
    }

    @Test
    void collationTraitDefInstanceMatchesCalciteStandard() {
        assertThat(PangrovaCollationTraitDef.INSTANCE)
                .as("PangrovaCollationTraitDef.INSTANCE must be the standard Calcite collation def")
                .isSameAs(RelCollationTraitDef.INSTANCE);
    }

    @Test
    void collationTraitDefSimpleName() {
        // Calcite's RelCollationTraitDef uses "sort" as its simple name.
        assertThat(PangrovaCollationTraitDef.INSTANCE.getSimpleName())
                .isEqualTo("sort");
    }
}

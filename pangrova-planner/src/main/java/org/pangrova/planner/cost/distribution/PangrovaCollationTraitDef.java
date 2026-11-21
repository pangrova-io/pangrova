package org.pangrova.planner.cost.distribution;

import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.RelCollationTraitDef;

/**
 * Registers Calcite's {@link RelCollationTraitDef} in the Pangrova CBO pipeline
 * and automatically inserts {@code PangrovaSort} enforcers when a parent operator
 * requires a collation that the child does not provide.
 *
 * <p>This enables the Volcano planner to:
 * <ul>
 *   <li>Propagate sort order from connector table scans (pre-sorted tables)</li>
 *   <li>Select merge join when both inputs are pre-sorted on join keys</li>
 *   <li>Eliminate redundant Sort operators when input is already ordered</li>
 * </ul>
 *
 * <p>Collation is a first-class Calcite trait (like Convention and Distribution),
 * so registering it means the planner tracks sort order for every RelNode in
 * the search space.</p>
 */
public final class PangrovaCollationTraitDef {

    private PangrovaCollationTraitDef() {
        // Utility class
    }

    /**
     * The standard Calcite collation trait definition. Registered alongside
     * {@link PangrovaDistributionTraitDef} in the Volcano planner so that
     * physical properties include both distribution and sort order.
     */
    public static final RelCollationTraitDef INSTANCE = RelCollationTraitDef.INSTANCE;

    /**
     * Registers the collation trait definition with the given planner.
     * Safe to call multiple times — Calcite deduplicates trait registrations.
     *
     * @param planner the Volcano planner to register with
     */
    public static void register(RelOptPlanner planner) {
        planner.addRelTraitDef(INSTANCE);
    }
}

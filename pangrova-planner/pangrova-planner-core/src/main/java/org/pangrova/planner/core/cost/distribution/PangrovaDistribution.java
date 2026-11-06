package org.pangrova.planner.core.cost.distribution;

import java.util.List;
import java.util.Objects;

import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitDef;

/**
 * Physical distribution trait for the Pangrova CBO.
 * Enables the Volcano planner to automatically insert Exchange enforcers
 * when distribution requirements mismatch between parent and child operators.
 *
 * <p>Variants: HASH (partitioned by key columns), BROADCAST (full copy to all nodes),
 * SINGLETON (single node), ROUND_ROBIN (evenly distributed), ANY (no requirement).</p>
 *
 * <p>HASH distribution carries key column indices so the enforcer knows which
 * columns to partition on when inserting an Exchange.</p>
 */
public final class PangrovaDistribution implements RelTrait {

    /**
     * Generic HASH distribution without specific key columns.
     * Used when the exact partition keys are not yet determined or for trait comparison.
     * Prefer {@link #hash(List)} with explicit keys in physical rules.
     */
    public static final PangrovaDistribution HASH = new PangrovaDistribution(Type.HASH, List.of());

    /** Full data copy broadcast to every node. */
    public static final PangrovaDistribution BROADCAST = new PangrovaDistribution(Type.BROADCAST, List.of());

    /** All data on a single node. */
    public static final PangrovaDistribution SINGLETON = new PangrovaDistribution(Type.SINGLETON, List.of());

    /** Data evenly distributed across nodes without key affinity. */
    public static final PangrovaDistribution ROUND_ROBIN = new PangrovaDistribution(Type.ROUND_ROBIN, List.of());

    /** No specific distribution requirement — any distribution satisfies this. */
    public static final PangrovaDistribution ANY = new PangrovaDistribution(Type.ANY, List.of());

    /**
     * Creates a HASH distribution partitioned by the given key column indices.
     * Used by physical join rules to declare required distribution on child inputs.
     *
     * @param keys column indices (0-based) that define the hash partition
     * @return HASH distribution carrying the specified key columns
     */
    public static PangrovaDistribution hash(List<Integer> keys) {
        Objects.requireNonNull(keys, "keys");
        if (keys.isEmpty()) {
            throw new IllegalArgumentException("HASH distribution requires at least one key column");
        }
        return new PangrovaDistribution(Type.HASH, List.copyOf(keys));
    }

    public enum Type {
        HASH,
        BROADCAST,
        SINGLETON,
        ROUND_ROBIN,
        ANY
    }

    private final Type type;
    private final List<Integer> keys;

    private PangrovaDistribution(Type type, List<Integer> keys) {
        this.type = type;
        this.keys = keys;
    }

    public Type type() {
        return type;
    }

    /**
     * Returns the hash partition key column indices.
     * Non-empty only for HASH distributions.
     */
    public List<Integer> keys() {
        return List.copyOf(keys);
    }

    @Override
    public RelTraitDef<PangrovaDistribution> getTraitDef() {
        return PangrovaDistributionTraitDef.INSTANCE;
    }

    /**
     * Returns true if this distribution satisfies the required trait.
     * ANY is satisfied by any distribution (it imposes no constraint).
     * HASH satisfies another HASH if the key sets are equal.
     * Otherwise, only an exact type match satisfies the requirement.
     */
    @Override
    public boolean satisfies(RelTrait trait) {
        if (!(trait instanceof PangrovaDistribution required)) {
            return false;
        }
        if (required.type == Type.ANY) {
            return true;
        }
        if (this.type != required.type) {
            return false;
        }
        if (this.type == Type.HASH) {
            return this.keys.equals(required.keys);
        }
        return true;
    }

    @Override
    public void register(RelOptPlanner planner) {
        // No additional registration needed
    }

    @Override
    public String toString() {
        if (type == Type.HASH && !keys.isEmpty()) {
            return "PangrovaDist.HASH" + keys;
        }
        return "PangrovaDist." + type.name();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PangrovaDistribution that)) return false;
        return type == that.type && keys.equals(that.keys);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, keys);
    }
}

package org.pangrova.metadata.core.dependency;

import java.util.Objects;

/**
 * A directed dependency edge: {@code child} depends on {@code parent}.
 *
 * <p>Read it as: "{@code child} requires {@code parent} to function." Dropping
 * {@code parent} without CASCADE breaks {@code child}. Examples:
 * <ul>
 *   <li>VIEW {@code analytics.user_summary} -> TABLE {@code raw.users} —
 *       parent is {@code raw.users}, child is {@code analytics.user_summary}.</li>
 *   <li>MATERIALIZED_VIEW {@code agg.daily_sales} -> TABLE {@code orders} —
 *       same direction.</li>
 *   <li>DYNAMIC_TABLE {@code stream.dt_orders} -> STREAM {@code kafka_orders}.</li>
 * </ul>
 *
 * <p>Self-edges are rejected: a non-recursive object cannot depend on itself.
 */
public record ObjectDependency(ObjectRef parent, ObjectRef child) {

    public ObjectDependency {
        Objects.requireNonNull(parent, "parent must not be null");
        Objects.requireNonNull(child, "child must not be null");
        if (parent.equals(child)) {
            throw new IllegalArgumentException("self-dependency is not allowed: " + parent);
        }
    }
}

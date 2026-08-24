package org.pangrova.planner.cost;

import java.util.List;
import java.util.Objects;

/**
 * Sealed interface describing how data is partitioned across nodes.
 */
public sealed interface Partitioning
        permits Partitioning.HashPartitioning,
                Partitioning.RangePartitioning,
                Partitioning.SinglePartitioning,
                Partitioning.AnyPartitioning {

    record HashPartitioning(List<String> columns, int bucketCount) implements Partitioning {
        public HashPartitioning {
            Objects.requireNonNull(columns, "columns must not be null");
            columns = List.copyOf(columns);
        }
    }

    record RangePartitioning(List<String> columns) implements Partitioning {
        public RangePartitioning {
            Objects.requireNonNull(columns, "columns must not be null");
            columns = List.copyOf(columns);
        }
    }

    record SinglePartitioning() implements Partitioning {}

    record AnyPartitioning() implements Partitioning {}
}

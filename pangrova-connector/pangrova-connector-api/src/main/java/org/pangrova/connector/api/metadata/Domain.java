package org.pangrova.connector.api.metadata;

import java.util.List;
import java.util.Set;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Represents a value domain constraint for a single column.
 *
 * <p>Used by the engine to push predicate information down to connectors,
 * enabling source-level filtering (partition pruning, file skipping, etc.).</p>
 */
@Experimental
public sealed interface Domain {

    /** Inclusive/exclusive range constraint. */
    record Range(
            Comparable<?> low,
            Comparable<?> high,
            boolean lowInclusive,
            boolean highInclusive
    ) implements Domain {

        public Range {
            if (low == null && high == null) {
                throw new IllegalArgumentException("Range must have at least one bound");
            }
        }
    }

    /** Values satisfying at least one of the ranges for this column. */
    record RangeUnion(List<Range> ranges) implements Domain {
        public RangeUnion {
            ranges = List.copyOf(ranges);
            if (ranges.isEmpty()) {
                throw new IllegalArgumentException("Range union requires at least one range");
            }
        }
    }

    /** Discrete value set constraint (IN-list). */
    record InList(Set<?> values) implements Domain {

        public InList {
            if (values == null || values.isEmpty()) {
                throw new IllegalArgumentException("InList values must not be null or empty");
            }
        }
    }

    /** Column must be NULL. */
    record IsNull() implements Domain {}

    /** Column must not be NULL. */
    record IsNotNull() implements Domain {}
}

package org.pangrova.connector.api.metadata;

import org.pangrova.connector.api.enums.DynamicFilterType;
import org.pangrova.common.api.annotation.Experimental;

import java.util.Optional;

/**
 * Represents a runtime filter that can be pushed down to a connector
 * during query execution.
 *
 * <p>Dynamic filters are generated from join conditions and applied to
 * the probe side to reduce the amount of data read from the connector.</p>
 */
@Experimental
public interface DynamicFilter {

    /** Returns the filter type. */
    DynamicFilterType filterType();

    /** Returns the target column name. */
    String column();

    /** Returns the minimum value for MIN_MAX filters, empty for BLOOM. */
    Optional<Comparable<?>> min();

    /** Returns the maximum value for MIN_MAX filters, empty for BLOOM. */
    Optional<Comparable<?>> max();

    /**
     * Tests whether a value might pass this filter.
     * For MIN_MAX: checks range containment.
     * For BLOOM: checks bloom filter membership.
     *
     * @return true if the value might match, false if definitely excluded
     */
    boolean mightContain(Object value);

    /** A no-op filter that accepts all values. */
    DynamicFilter ALL = new DynamicFilter() {
        @Override
        public DynamicFilterType filterType() {
            return DynamicFilterType.MIN_MAX;
        }

        @Override
        public String column() {
            return "";
        }

        @Override
        public Optional<Comparable<?>> min() {
            return Optional.empty();
        }

        @Override
        public Optional<Comparable<?>> max() {
            return Optional.empty();
        }

        @Override
        public boolean mightContain(Object value) {
            return true;
        }
    };
}

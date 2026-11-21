package org.pangrova.connector.api.index.predicate;

import org.pangrova.common.api.annotation.Experimental;

/**
 * A predicate that can be evaluated against an index to determine whether
 * a data file may contain matching rows.
 *
 * <p>Each predicate targets a specific column and carries the parameters
 * needed for the corresponding index evaluation strategy.
 *
 * @see org.pangrova.connector.api.enums.IndexEvalResult
 */
@Experimental
public sealed interface IndexPredicate
        permits EqualTo, RangeQuery, InList, TextMatch, LikePattern,
                IsNull, IsNotNull, SpatialQuery, ContainsElement, OrPredicate {

    /**
     * Returns the column name this predicate targets.
     *
     * @return non-blank column name
     */
    String column();
}

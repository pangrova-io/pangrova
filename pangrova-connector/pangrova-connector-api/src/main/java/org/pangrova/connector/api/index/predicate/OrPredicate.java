package org.pangrova.connector.api.index.predicate;

import java.util.List;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Disjunction of index predicates on the same column.
 *
 * <p>A file can be skipped only if ALL branches would skip it.
 * If any branch matches, the file must be kept.</p>
 *
 * @param column   the column all branches target
 * @param branches the disjunct predicates (at least 2)
 */
@Experimental
public record OrPredicate(String column, List<IndexPredicate> branches) implements IndexPredicate {

    public OrPredicate {
        if (branches == null || branches.size() < 2) {
            throw new IllegalArgumentException("OrPredicate requires at least 2 branches");
        }
        branches = List.copyOf(branches);
    }
}

package org.pangrova.planner.substrait;

import io.substrait.expression.Expression;
import org.apache.calcite.rel.RelFieldCollation;

/** Preserves direction and effective NULL ordering for ordinary and window sorts. */
final class SortDirectionMapper {

    private SortDirectionMapper() {
    }

    static Expression.SortDirection map(RelFieldCollation.Direction direction,
                                        RelFieldCollation.NullDirection nullDirection) {
        boolean descending = direction.isDescending();
        var effectiveNullDirection = nullDirection == RelFieldCollation.NullDirection.UNSPECIFIED
                ? direction.defaultNullDirection() : nullDirection;
        boolean nullsFirst = effectiveNullDirection == RelFieldCollation.NullDirection.FIRST;
        return descending
                ? (nullsFirst ? Expression.SortDirection.DESC_NULLS_FIRST
                             : Expression.SortDirection.DESC_NULLS_LAST)
                : (nullsFirst ? Expression.SortDirection.ASC_NULLS_FIRST
                             : Expression.SortDirection.ASC_NULLS_LAST);
    }
}

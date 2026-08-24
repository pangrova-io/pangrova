package org.pangrova.connector.iceberg;

import org.pangrova.spi.connector.metadata.table.ColumnHandle;
import org.pangrova.spi.connector.metadata.Constraint;
import org.pangrova.spi.connector.metadata.Domain;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Translates SPI {@link Constraint} domains into Iceberg {@link Expression}s
 * for predicate pushdown during split planning.
 */
final class IcebergConstraintExpressions {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergConstraintExpressions.class);

    private IcebergConstraintExpressions() {}

    /**
     * Builds a combined Iceberg expression from the constraint's domains, or
     * {@code null} when the constraint carries no pushable domains.
     */
    static Expression fromConstraint(Constraint constraint) {
        if (constraint == null || constraint == Constraint.EMPTY) {
            return null;
        }
        Map<ColumnHandle, Domain> domains = constraint.getDomains();
        if (domains.isEmpty()) {
            return null;
        }
        return buildCombinedExpression(domains);
    }

    private static Expression buildCombinedExpression(Map<ColumnHandle, Domain> domains) {
        Expression combined = Expressions.alwaysTrue();
        for (var entry : domains.entrySet()) {
            String columnName = entry.getKey().columnName();
            try {
                Expression expr = domainToIcebergExpression(columnName, entry.getValue());
                if (expr != null) {
                    combined = Expressions.and(combined, expr);
                }
            } catch (Exception e) {
                LOG.debug("event=skip_domain_expression column={} error={}", columnName, e.getMessage());
            }
        }
        return combined;
    }

    private static Expression domainToIcebergExpression(String column, Domain domain) {
        if (domain instanceof Domain.Range range) {
            return rangeToExpression(column, range);
        }
        if (domain instanceof Domain.InList inList) {
            return Expressions.in(column, inList.values().toArray());
        }
        if (domain instanceof Domain.IsNull) {
            return Expressions.isNull(column);
        }
        if (domain instanceof Domain.IsNotNull) {
            return Expressions.notNull(column);
        }
        return Expressions.alwaysTrue();
    }

    private static Expression rangeToExpression(String column, Domain.Range range) {
        if (isSingleValueRange(range)) {
            return Expressions.equal(column, range.low());
        }
        Expression expr = lowerBoundExpression(column, range);
        Expression upper = upperBoundExpression(column, range);
        if (upper != null) {
            expr = Expressions.and(expr, upper);
        }
        return expr;
    }

    private static boolean isSingleValueRange(Domain.Range range) {
        return range.low() != null && range.high() != null
                && range.low().equals(range.high())
                && range.lowInclusive() && range.highInclusive();
    }

    private static Expression lowerBoundExpression(String column, Domain.Range range) {
        if (range.low() == null) {
            return Expressions.alwaysTrue();
        }
        return range.lowInclusive()
                ? Expressions.greaterThanOrEqual(column, range.low())
                : Expressions.greaterThan(column, range.low());
    }

    private static Expression upperBoundExpression(String column, Domain.Range range) {
        if (range.high() == null) {
            return null;
        }
        return range.highInclusive()
                ? Expressions.lessThanOrEqual(column, range.high())
                : Expressions.lessThan(column, range.high());
    }
}

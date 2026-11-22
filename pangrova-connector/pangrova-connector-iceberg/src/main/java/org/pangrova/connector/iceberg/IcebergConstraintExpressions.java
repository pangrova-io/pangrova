package org.pangrova.connector.iceberg;

import org.pangrova.connector.api.metadata.table.ColumnHandle;
import org.pangrova.connector.api.metadata.Constraint;
import org.pangrova.connector.api.metadata.Domain;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Type;
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
    static Expression fromConstraint(Constraint constraint, Schema schema) {
        if (constraint == null || constraint == Constraint.EMPTY) {
            return null;
        }
        Map<ColumnHandle, Domain> domains = constraint.getDomains();
        if (domains.isEmpty()) {
            return null;
        }
        return buildCombinedExpression(domains, schema);
    }

    private static Expression buildCombinedExpression(Map<ColumnHandle, Domain> domains, Schema schema) {
        Expression combined = Expressions.alwaysTrue();
        for (var entry : domains.entrySet()) {
            String columnName = entry.getKey().columnName();
            Type type = schema.findType(columnName);
            // UUID bounds have no persisted ordering provenance. Older Iceberg writers can
            // mix signed Java UUID aggregation with unsigned Parquet bounds, so even
            // apparently valid min/max pairs can omit real values. Keep value predicates
            // in Native execution, where Parquet statistics have canonical byte ordering.
            if (type != null && type.typeId() == Type.TypeID.UUID
                    && !(entry.getValue() instanceof Domain.IsNull)
                    && !(entry.getValue() instanceof Domain.IsNotNull)) {
                continue;
            }
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
        if (domain instanceof Domain.RangeUnion union) {
            Expression expression = Expressions.alwaysFalse();
            for (Domain.Range range : union.ranges()) {
                expression = Expressions.or(expression, rangeToExpression(column, range));
            }
            return expression;
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

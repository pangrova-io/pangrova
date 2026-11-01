package org.pangrova.connector.iceberg;

import org.pangrova.spi.connector.metadata.DeleteFilter;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.math.BigDecimal;

/**
 * Translates connector-agnostic {@link DeleteFilter} expressions into Iceberg
 * {@link Expression}s, coercing filter literal values to the exact Java types
 * expected by the Iceberg Expressions API for the target table schema.
 */
final class IcebergDeleteFilterTranslator {

    private IcebergDeleteFilterTranslator() {}

    /**
     * Converts a connector-agnostic DeleteFilter to an Iceberg Expression.
     * Uses the table schema to coerce filter literal values to the expected
     * Iceberg type (e.g., BigDecimal → Double for DOUBLE columns).
     */
    static Expression toIcebergExpression(DeleteFilter filter, Schema schema) {
        if (filter instanceof DeleteFilter.All) {
            return Expressions.alwaysTrue();
        }
        Expression comparison = convertComparison(filter, schema);
        if (comparison != null) {
            return comparison;
        }
        Expression composite = convertComposite(filter, schema);
        if (composite != null) {
            return composite;
        }
        Expression unary = convertUnary(filter, schema);
        if (unary != null) {
            return unary;
        }
        throw new UnsupportedOperationException("Unknown DeleteFilter type: " + filter.getClass());
    }

    private static Expression convertComparison(DeleteFilter filter, Schema schema) {
        if (filter instanceof DeleteFilter.Equal f) {
            return Expressions.equal(f.column(), coerceValue(f.column(), f.value(), schema));
        } else if (filter instanceof DeleteFilter.NotEqual f) {
            return Expressions.notEqual(f.column(), coerceValue(f.column(), f.value(), schema));
        } else if (filter instanceof DeleteFilter.LessThan f) {
            return Expressions.lessThan(f.column(), coerceValue(f.column(), f.value(), schema));
        } else if (filter instanceof DeleteFilter.LessThanOrEqual f) {
            return Expressions.lessThanOrEqual(f.column(), coerceValue(f.column(), f.value(), schema));
        } else if (filter instanceof DeleteFilter.GreaterThan f) {
            return Expressions.greaterThan(f.column(), coerceValue(f.column(), f.value(), schema));
        } else if (filter instanceof DeleteFilter.GreaterThanOrEqual f) {
            return Expressions.greaterThanOrEqual(f.column(), coerceValue(f.column(), f.value(), schema));
        }
        return null;
    }

    private static Expression convertComposite(DeleteFilter filter, Schema schema) {
        if (filter instanceof DeleteFilter.And f) {
            return f.filters().stream()
                    .map(sub -> toIcebergExpression(sub, schema))
                    .reduce(Expressions.alwaysTrue(),
                            Expressions::and);
        } else if (filter instanceof DeleteFilter.Or f) {
            return f.filters().stream()
                    .map(sub -> toIcebergExpression(sub, schema))
                    .reduce(Expressions.alwaysFalse(),
                            Expressions::or);
        } else if (filter instanceof DeleteFilter.In f) {
            Object[] coerced = f.values().stream()
                    .map(v -> coerceValue(f.column(), v, schema))
                    .toArray();
            return Expressions.in(f.column(), coerced);
        }
        return null;
    }

    private static Expression convertUnary(DeleteFilter filter, Schema schema) {
        if (filter instanceof DeleteFilter.Not f) {
            return Expressions.not(toIcebergExpression(f.filter(), schema));
        } else if (filter instanceof DeleteFilter.IsNull f) {
            return Expressions.isNull(f.column());
        } else if (filter instanceof DeleteFilter.IsNotNull f) {
            return Expressions.notNull(f.column());
        }
        return null;
    }

    /**
     * Coerces a filter literal value to match the Iceberg column type.
     * Iceberg's Expressions API requires values to be the exact Java type
     * corresponding to the column's Iceberg type (Double for DOUBLE, Float for
     * FLOAT, Long for LONG, Integer for INT). SQL parsers may produce BigDecimal
     * or Long for numeric literals, so we convert here.
     */
    static Object coerceValue(String column, Object value, Schema schema) {
        if (value == null || schema == null) {
            return value;
        }
        Types.NestedField field = schema.findField(column);
        if (field == null) {
            return value;
        }
        Type type = field.type();
        if (value instanceof BigDecimal bd) {
            return coerceBigDecimal(bd, type);
        }
        if (value instanceof Number num) {
            return coerceNumber(num, type);
        }
        return value;
    }

    private static Object coerceBigDecimal(BigDecimal bd, Type type) {
        if (type.typeId() == Type.TypeID.DOUBLE) {
            return bd.doubleValue();
        } else if (type.typeId() == Type.TypeID.FLOAT) {
            return bd.floatValue();
        } else if (type.typeId() == Type.TypeID.LONG) {
            return bd.longValueExact();
        } else if (type.typeId() == Type.TypeID.INTEGER) {
            return bd.intValueExact();
        }
        return bd;
    }

    private static Object coerceNumber(Number num, Type type) {
        return switch (type.typeId()) {
            case DOUBLE -> num instanceof Double ? num : num.doubleValue();
            case FLOAT -> num instanceof Float ? num : num.floatValue();
            case LONG -> num instanceof Long ? num : num.longValue();
            case INTEGER -> num instanceof Integer ? num : num.intValue();
            default -> num;
        };
    }
}

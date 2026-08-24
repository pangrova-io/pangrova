package org.pangrova.protocol.index;

import org.pangrova.protocol.v1.ComparableValue;
import org.pangrova.protocol.v1.ContainsElementPredicate;
import org.pangrova.protocol.v1.EqualToPredicate;
import org.pangrova.protocol.v1.InListPredicate;
import org.pangrova.protocol.v1.IndexPredicateInfo;
import org.pangrova.protocol.v1.IsNotNullPredicate;
import org.pangrova.protocol.v1.IsNullPredicate;
import org.pangrova.protocol.v1.LikePatternPredicate;
import org.pangrova.protocol.v1.RangeQueryPredicate;
import org.pangrova.protocol.v1.SpatialQueryPredicate;
import org.pangrova.protocol.v1.TextMatchPredicate;
import org.pangrova.spi.index.predicate.ContainmentMode;
import org.pangrova.spi.index.predicate.ContainsElement;
import org.pangrova.spi.index.predicate.EqualTo;
import org.pangrova.spi.index.predicate.InList;
import org.pangrova.spi.index.predicate.IndexPredicate;
import org.pangrova.spi.index.predicate.IsNotNull;
import org.pangrova.spi.index.predicate.IsNull;
import org.pangrova.spi.index.predicate.LikePattern;
import org.pangrova.spi.index.predicate.RangeQuery;
import org.pangrova.spi.index.predicate.SpatialOp;
import org.pangrova.spi.index.predicate.SpatialQuery;
import org.pangrova.spi.index.predicate.TextMatch;
import org.pangrova.spi.index.predicate.TextMatchMode;

import java.util.ArrayList;
import java.util.List;

/**
 * Converts between SPI {@link IndexPredicate} objects and proto
 * {@link IndexPredicateInfo} messages for structured wire transport.
 *
 * <p>Replaces the legacy JSON string encoding (predicate_json) with
 * proper typed proto messages, enabling schema validation and
 * cross-language compatibility without custom text parsing.</p>
 */
public final class IndexPredicateProtoConverter {

    private IndexPredicateProtoConverter() {}

    /**
     * Converts an SPI IndexPredicate to a proto IndexPredicateInfo message.
     */
    public static IndexPredicateInfo toProto(IndexPredicate predicate) {
        IndexPredicateInfo.Builder builder = IndexPredicateInfo.newBuilder()
                .setColumn(predicate.column());
        if (!applyCorePredicate(builder, predicate) && !applyExtendedPredicate(builder, predicate)) {
            throw new IllegalArgumentException("Unknown predicate type: " + predicate.getClass().getName());
        }
        return builder.build();
    }

    private static boolean applyCorePredicate(IndexPredicateInfo.Builder builder, IndexPredicate predicate) {
        if (predicate instanceof EqualTo eq) {
            builder.setEqualTo(EqualToPredicate.newBuilder()
                    .setValue(toComparableValue(eq.value())));
            return true;
        }
        if (predicate instanceof RangeQuery rq) {
            builder.setRangeQuery(toRangeQueryPredicate(rq));
            return true;
        }
        if (predicate instanceof InList in) {
            builder.setInList(toInListPredicate(in));
            return true;
        }
        if (predicate instanceof TextMatch tm) {
            builder.setTextMatch(TextMatchPredicate.newBuilder()
                    .setMode(tm.mode().name())
                    .addAllTokens(tm.tokens()));
            return true;
        }
        if (predicate instanceof LikePattern lp) {
            builder.setLikePattern(LikePatternPredicate.newBuilder()
                    .setPattern(lp.pattern()));
            return true;
        }
        return false;
    }

    private static boolean applyExtendedPredicate(IndexPredicateInfo.Builder builder, IndexPredicate predicate) {
        if (predicate instanceof IsNull) {
            builder.setIsNull(IsNullPredicate.getDefaultInstance());
            return true;
        }
        if (predicate instanceof IsNotNull) {
            builder.setIsNotNull(IsNotNullPredicate.getDefaultInstance());
            return true;
        }
        if (predicate instanceof SpatialQuery sq) {
            builder.setSpatialQuery(SpatialQueryPredicate.newBuilder()
                    .setMinX(sq.minX()).setMinY(sq.minY())
                    .setMaxX(sq.maxX()).setMaxY(sq.maxY())
                    .setOp(sq.op().name()));
            return true;
        }
        if (predicate instanceof ContainsElement ce) {
            builder.setContainsElement(toContainsElementPredicate(ce));
            return true;
        }
        return false;
    }

    private static RangeQueryPredicate toRangeQueryPredicate(RangeQuery rq) {
        RangeQueryPredicate.Builder rqb = RangeQueryPredicate.newBuilder()
                .setLowerInclusive(rq.lowerInclusive())
                .setUpperInclusive(rq.upperInclusive());
        if (rq.lower() != null) {
            rqb.setLower(toComparableValue(rq.lower()));
        }
        if (rq.upper() != null) {
            rqb.setUpper(toComparableValue(rq.upper()));
        }
        return rqb.build();
    }

    private static InListPredicate toInListPredicate(InList in) {
        InListPredicate.Builder ilb = InListPredicate.newBuilder();
        for (Comparable<?> v : in.values()) {
            ilb.addValues(toComparableValue(v));
        }
        return ilb.build();
    }

    private static ContainsElementPredicate toContainsElementPredicate(ContainsElement ce) {
        ContainsElementPredicate.Builder ceb = ContainsElementPredicate.newBuilder()
                .setMode(ce.mode().name());
        for (Comparable<?> v : ce.elements()) {
            ceb.addElements(toComparableValue(v));
        }
        return ceb.build();
    }

    /**
     * Converts a proto IndexPredicateInfo message back to an SPI IndexPredicate.
     */
    public static IndexPredicate fromProto(IndexPredicateInfo proto) {
        String column = proto.getColumn();

        return switch (proto.getPredicateTypeCase()) {
            case EQUAL_TO -> new EqualTo(column, fromComparableValue(proto.getEqualTo().getValue()));
            case RANGE_QUERY -> fromRangeQuery(column, proto.getRangeQuery());
            case IN_LIST -> new InList(column, fromComparableValues(proto.getInList().getValuesList()));
            case TEXT_MATCH -> new TextMatch(column, proto.getTextMatch().getTokensList(),
                    TextMatchMode.valueOf(proto.getTextMatch().getMode()));
            case LIKE_PATTERN -> new LikePattern(column, proto.getLikePattern().getPattern());
            case IS_NULL -> new IsNull(column);
            case IS_NOT_NULL -> new IsNotNull(column);
            case SPATIAL_QUERY -> fromSpatialQuery(column, proto.getSpatialQuery());
            case CONTAINS_ELEMENT -> new ContainsElement(column,
                    fromComparableValues(proto.getContainsElement().getElementsList()),
                    ContainmentMode.valueOf(proto.getContainsElement().getMode()));
            case PREDICATETYPE_NOT_SET -> throw new IllegalArgumentException(
                    "IndexPredicateInfo has no predicate type set for column: " + column);
        };
    }

    private static RangeQuery fromRangeQuery(String column, RangeQueryPredicate rq) {
        Comparable<?> lower = rq.hasLower() ? fromComparableValue(rq.getLower()) : null;
        Comparable<?> upper = rq.hasUpper() ? fromComparableValue(rq.getUpper()) : null;
        return new RangeQuery(column, lower, upper, rq.getLowerInclusive(), rq.getUpperInclusive());
    }

    private static SpatialQuery fromSpatialQuery(String column, SpatialQueryPredicate sq) {
        return new SpatialQuery(column, sq.getMinX(), sq.getMinY(),
                sq.getMaxX(), sq.getMaxY(), SpatialOp.valueOf(sq.getOp()));
    }

    private static ComparableValue toComparableValue(Comparable<?> value) {
        ComparableValue.Builder builder = ComparableValue.newBuilder();
        if (value instanceof String s) {
            builder.setStringValue(s);
        } else if (value instanceof Long l) {
            builder.setLongValue(l);
        } else if (value instanceof Integer i) {
            builder.setIntValue(i);
        } else if (value instanceof Double d) {
            builder.setDoubleValue(d);
        } else if (value instanceof Float f) {
            builder.setFloatValue(f);
        } else if (value instanceof Boolean b) {
            builder.setBoolValue(b);
        } else {
            builder.setStringValue(value.toString());
        }
        return builder.build();
    }

    private static Comparable<?> fromComparableValue(ComparableValue value) {
        return switch (value.getValueTypeCase()) {
            case STRING_VALUE -> value.getStringValue();
            case LONG_VALUE -> value.getLongValue();
            case INT_VALUE -> value.getIntValue();
            case DOUBLE_VALUE -> value.getDoubleValue();
            case FLOAT_VALUE -> value.getFloatValue();
            case BOOL_VALUE -> value.getBoolValue();
            case VALUETYPE_NOT_SET -> throw new IllegalArgumentException("ComparableValue has no type set");
        };
    }

    private static List<Comparable<?>> fromComparableValues(List<ComparableValue> values) {
        List<Comparable<?>> result = new ArrayList<>(values.size());
        for (ComparableValue v : values) {
            result.add(fromComparableValue(v));
        }
        return List.copyOf(result);
    }
}

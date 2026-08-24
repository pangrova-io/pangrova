package org.pangrova.protocol.index;

import org.pangrova.spi.index.predicate.ContainsElement;
import org.pangrova.spi.index.predicate.EqualTo;
import org.pangrova.spi.index.predicate.InList;
import org.pangrova.spi.index.predicate.IndexPredicate;
import org.pangrova.spi.index.predicate.IsNotNull;
import org.pangrova.spi.index.predicate.IsNull;
import org.pangrova.spi.index.predicate.LikePattern;
import org.pangrova.spi.index.predicate.RangeQuery;
import org.pangrova.spi.index.predicate.SpatialQuery;
import org.pangrova.spi.index.predicate.TextMatch;

import java.util.List;

/**
 * Serializes {@link IndexPredicate} objects to the JSON format expected by
 * the native C++ {@code IndexHintFilter::parsePredicate}.
 *
 * <p>Supported JSON formats (matching native expectations):
 * <ul>
 *   <li>{@code {"op":"eq","column":"col","value":"val"}}</li>
 *   <li>{@code {"op":"in","column":"col","values":["v1","v2"]}}</li>
 *   <li>{@code {"op":"range","column":"col","min":10,"max":99}}</li>
 * </ul>
 *
 * <p>Predicates not recognized by the native parser are serialized with
 * a best-effort format; the native side treats unrecognized ops as
 * {@code UNSUPPORTED} and conservatively keeps the split.</p>
 *
 * <p>This class does NOT use a JSON library to avoid adding dependencies
 * to the protocol module. The output is trivially structured and safe to
 * produce with string concatenation for the limited predicate vocabulary.</p>
 */
public final class IndexPredicateJsonSerializer {

    private IndexPredicateJsonSerializer() {}

    /**
     * Serializes a predicate to the JSON format consumed by native IndexHintFilter.
     *
     * @param predicate the index predicate to serialize
     * @return JSON string matching native parser expectations
     */
    public static String toJson(IndexPredicate predicate) {
        if (predicate instanceof EqualTo eq) {
            return "{\"op\":\"eq\",\"column\":" + jsonString(eq.column())
                    + ",\"value\":" + jsonValue(eq.value()) + "}";
        }
        if (predicate instanceof InList in) {
            return "{\"op\":\"in\",\"column\":" + jsonString(in.column())
                    + ",\"values\":" + jsonArray(in.values()) + "}";
        }
        if (predicate instanceof RangeQuery rq) {
            return serializeRange(rq);
        }
        return serializeExtendedPredicate(predicate);
    }

    private static String serializeExtendedPredicate(IndexPredicate predicate) {
        if (predicate instanceof IsNull isn) {
            return "{\"op\":\"is_null\",\"column\":" + jsonString(isn.column()) + "}";
        }
        if (predicate instanceof IsNotNull inn) {
            return "{\"op\":\"is_not_null\",\"column\":" + jsonString(inn.column()) + "}";
        }
        if (predicate instanceof TextMatch tm) {
            return "{\"op\":\"text_match\",\"column\":" + jsonString(tm.column())
                    + ",\"mode\":" + jsonString(tm.mode().name())
                    + ",\"tokens\":" + jsonStringArray(tm.tokens()) + "}";
        }
        if (predicate instanceof LikePattern lp) {
            return "{\"op\":\"like\",\"column\":" + jsonString(lp.column())
                    + ",\"pattern\":" + jsonString(lp.pattern()) + "}";
        }
        if (predicate instanceof SpatialQuery sq) {
            return "{\"op\":\"spatial\",\"column\":" + jsonString(sq.column())
                    + ",\"min_x\":" + sq.minX()
                    + ",\"min_y\":" + sq.minY()
                    + ",\"max_x\":" + sq.maxX()
                    + ",\"max_y\":" + sq.maxY()
                    + ",\"spatial_op\":" + jsonString(sq.op().name()) + "}";
        }
        if (predicate instanceof ContainsElement ce) {
            return "{\"op\":\"contains\",\"column\":" + jsonString(ce.column())
                    + ",\"elements\":" + jsonArray(ce.elements())
                    + ",\"mode\":" + jsonString(ce.mode().name()) + "}";
        }
        // Fallback: serialize as unsupported so native conservatively keeps the split
        return "{\"op\":\"unsupported\",\"column\":" + jsonString(predicate.column()) + "}";
    }

    private static String serializeRange(RangeQuery rq) {
        StringBuilder sb = new StringBuilder();
        sb.append("{\"op\":\"range\",\"column\":").append(jsonString(rq.column()));
        if (rq.lower() != null) {
            sb.append(",\"min\":").append(jsonValue(rq.lower()));
        }
        if (rq.upper() != null) {
            sb.append(",\"max\":").append(jsonValue(rq.upper()));
        }
        sb.append("}");
        return sb.toString();
    }

    private static String jsonString(String s) {
        if (s == null) {
            return "null";
        }
        StringBuilder sb = new StringBuilder(s.length() + 2);
        sb.append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> sb.append(c);
            }
        }
        sb.append('"');
        return sb.toString();
    }

    private static String jsonValue(Comparable<?> value) {
        if (value instanceof String s) {
            return jsonString(s);
        }
        if (value instanceof Number) {
            return value.toString();
        }
        if (value instanceof Boolean) {
            return value.toString();
        }
        return jsonString(value.toString());
    }

    private static String jsonArray(List<Comparable<?>> values) {
        StringBuilder sb = new StringBuilder();
        sb.append('[');
        for (int i = 0; i < values.size(); i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append(jsonValue(values.get(i)));
        }
        sb.append(']');
        return sb.toString();
    }

    private static String jsonStringArray(List<String> values) {
        StringBuilder sb = new StringBuilder();
        sb.append('[');
        for (int i = 0; i < values.size(); i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append(jsonString(values.get(i)));
        }
        sb.append(']');
        return sb.toString();
    }
}

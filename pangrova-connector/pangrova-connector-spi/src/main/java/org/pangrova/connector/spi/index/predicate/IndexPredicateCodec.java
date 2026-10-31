package org.pangrova.connector.spi.index.predicate;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Serializes and deserializes {@link IndexPredicate} to/from a compact
 * text format for wire transport (proto string fields).
 *
 * <p>Format: {@code TYPE\tcolumn\tparams} where params are type-specific.
 * Comparable values are encoded as {@code type|value} (e.g., {@code S|hello},
 * {@code L|42}, {@code D|3.14}).</p>
 *
 * <p>This codec lives in pangrova-connector-spi so both coordinator and worker can
 * use it without additional dependencies.</p>
 */
@Experimental
public final class IndexPredicateCodec {

    private static final char SEP = '\t';
    private static final char VAL_TYPE_SEP = '|';
    private static final char LIST_SEP = ',';
    private static final Pattern ESCAPE = Pattern.compile("[\t,|\\\\]");

    private IndexPredicateCodec() {}

    /** Serializes an IndexPredicate to a compact text string. */
    public static String serialize(IndexPredicate predicate) {
        if (predicate instanceof EqualTo eq) {
            return "EQ" + SEP + escape(eq.column()) + SEP + encodeValue(eq.value());
        }
        if (predicate instanceof RangeQuery rq) {
            return serializeRange(rq);
        }
        if (predicate instanceof InList in) {
            return "IN" + SEP + escape(in.column())
                    + SEP + encodeValueList(in.values());
        }
        return serializeOtherPredicate(predicate);
    }

    private static String serializeRange(RangeQuery rq) {
        return "RNG" + SEP + escape(rq.column())
                + SEP + (rq.lower() != null ? encodeValue(rq.lower()) : "")
                + SEP + (rq.upper() != null ? encodeValue(rq.upper()) : "")
                + SEP + (rq.lowerInclusive() ? "1" : "0")
                + SEP + (rq.upperInclusive() ? "1" : "0");
    }

    private static String serializeOtherPredicate(IndexPredicate predicate) {
        if (predicate instanceof TextMatch tm) {
            return "TXT" + SEP + escape(tm.column())
                    + SEP + tm.mode().name()
                    + SEP + encodeStringList(tm.tokens());
        }
        if (predicate instanceof LikePattern lp) {
            return "LIKE" + SEP + escape(lp.column())
                    + SEP + escape(lp.pattern());
        }
        if (predicate instanceof IsNull isn) {
            return "NULL" + SEP + escape(isn.column());
        }
        return serializeAdvancedPredicate(predicate);
    }

    private static String serializeAdvancedPredicate(IndexPredicate predicate) {
        if (predicate instanceof IsNotNull inn) {
            return "NNULL" + SEP + escape(inn.column());
        }
        if (predicate instanceof SpatialQuery sq) {
            return "SPAT" + SEP + escape(sq.column())
                    + SEP + sq.minX() + SEP + sq.minY()
                    + SEP + sq.maxX() + SEP + sq.maxY()
                    + SEP + sq.op().name();
        }
        if (predicate instanceof ContainsElement ce) {
            return "CONT" + SEP + escape(ce.column())
                    + SEP + encodeValueList(ce.elements())
                    + SEP + ce.mode().name();
        }
        throw new IllegalArgumentException("Unknown predicate type: "
                + predicate.getClass().getName());
    }

    /** Deserializes an IndexPredicate from the compact text format. */
    public static IndexPredicate deserialize(String text) {
        String[] parts = text.split("\t", -1);
        if (parts.length < 2) {
            throw new IllegalArgumentException("Invalid predicate format: " + text);
        }
        String type = parts[0];
        String column = unescape(parts[1]);
        return switch (type) {
            case "EQ" -> new EqualTo(column, decodeValue(parts[2]));
            case "RNG" -> deserializeRange(column, parts);
            case "IN" -> new InList(column, decodeValueList(parts[2]));
            case "TXT" -> new TextMatch(column, decodeStringList(parts[3]),
                    TextMatchMode.valueOf(parts[2]));
            case "LIKE" -> new LikePattern(column, unescape(parts[2]));
            case "NULL" -> new IsNull(column);
            case "NNULL" -> new IsNotNull(column);
            case "SPAT" -> deserializeSpatial(column, parts);
            case "CONT" -> new ContainsElement(column,
                    decodeValueList(parts[2]), ContainmentMode.valueOf(parts[3]));
            default -> throw new IllegalArgumentException("Unknown predicate type: " + type);
        };
    }

    private static RangeQuery deserializeRange(String column, String[] parts) {
        return new RangeQuery(column,
                parts[2].isEmpty() ? null : decodeValue(parts[2]),
                parts[3].isEmpty() ? null : decodeValue(parts[3]),
                "1".equals(parts[4]), "1".equals(parts[5]));
    }

    private static SpatialQuery deserializeSpatial(String column, String[] parts) {
        return new SpatialQuery(column,
                Double.parseDouble(parts[2]), Double.parseDouble(parts[3]),
                Double.parseDouble(parts[4]), Double.parseDouble(parts[5]),
                SpatialOp.valueOf(parts[6]));
    }

    // --- Value encoding ---

    private static String encodeValue(Comparable<?> value) {
        if (value instanceof String s) {
            return "S" + VAL_TYPE_SEP + escape(s);
        }
        if (value instanceof Number) {
            return encodeNumber(value);
        }
        if (value instanceof Boolean b) {
            return "B" + VAL_TYPE_SEP + b;
        }
        return "S" + VAL_TYPE_SEP + escape(value.toString());
    }

    private static String encodeNumber(Comparable<?> value) {
        if (value instanceof Long l) {
            return "L" + VAL_TYPE_SEP + l;
        }
        if (value instanceof Integer i) {
            return "I" + VAL_TYPE_SEP + i;
        }
        return encodeFloatingOrDefault(value);
    }

    private static String encodeFloatingOrDefault(Comparable<?> value) {
        if (value instanceof Double d) {
            return "D" + VAL_TYPE_SEP + d;
        }
        if (value instanceof Float f) {
            return "F" + VAL_TYPE_SEP + f;
        }
        return "S" + VAL_TYPE_SEP + escape(value.toString());
    }

    private static Comparable<?> decodeValue(String encoded) {
        int sep = encoded.indexOf(VAL_TYPE_SEP);
        if (sep < 0) {
            throw new IllegalArgumentException("Invalid value encoding: " + encoded);
        }
        String typeTag = encoded.substring(0, sep);
        String raw = encoded.substring(sep + 1);
        switch (typeTag) {
            case "S": return unescape(raw);
            case "L": return Long.parseLong(raw);
            case "I": return Integer.parseInt(raw);
            case "D": return Double.parseDouble(raw);
            case "F": return Float.parseFloat(raw);
            case "B": return Boolean.parseBoolean(raw);
            default: return unescape(raw);
        }
    }

    private static String encodeValueList(List<Comparable<?>> values) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < values.size(); i++) {
            if (i > 0) sb.append(LIST_SEP);
            sb.append(encodeValue(values.get(i)));
        }
        return sb.toString();
    }

    private static List<Comparable<?>> decodeValueList(String encoded) {
        if (encoded.isEmpty()) return List.of();
        List<Comparable<?>> result = new ArrayList<>();
        for (String part : splitPreservingEscapes(encoded)) {
            result.add(decodeValue(part));
        }
        return result;
    }

    private static String encodeStringList(List<String> strings) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < strings.size(); i++) {
            if (i > 0) sb.append(LIST_SEP);
            sb.append(escape(strings.get(i)));
        }
        return sb.toString();
    }

    private static List<String> decodeStringList(String encoded) {
        if (encoded.isEmpty()) return List.of();
        List<String> result = new ArrayList<>();
        for (String part : splitPreservingEscapes(encoded)) {
            result.add(unescape(part));
        }
        return result;
    }

    // --- Escaping ---

    private static String escape(String s) {
        Matcher m = ESCAPE.matcher(s);
        StringBuilder sb = new StringBuilder();
        while (m.find()) {
            m.appendReplacement(sb, "\\\\" + m.group());
        }
        m.appendTail(sb);
        return sb.toString();
    }

    private static String unescape(String s) {
        StringBuilder sb = new StringBuilder(s.length());
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '\\' && i + 1 < s.length()) {
                sb.append(s.charAt(++i));
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }

    /**
     * Splits by comma but respects backslash-escaped commas.
     */
    private static List<String> splitPreservingEscapes(String s) {
        List<String> parts = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '\\' && i + 1 < s.length()) {
                current.append(c).append(s.charAt(++i));
            } else if (c == LIST_SEP) {
                parts.add(current.toString());
                current.setLength(0);
            } else {
                current.append(c);
            }
        }
        parts.add(current.toString());
        return parts;
    }
}

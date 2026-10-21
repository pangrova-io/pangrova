package org.pangrova.coordinator.query.statement.dml;

import java.util.function.BiFunction;
import org.pangrova.parser.ast.Expression;
import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.Literal;
import org.pangrova.parser.ast.LiteralValue;
import org.pangrova.parser.ast.expression.BinaryExpression;
import org.pangrova.parser.ast.expression.CastExpression;
import org.pangrova.parser.ast.expression.InExpression;
import org.pangrova.parser.ast.expression.IsNullExpression;
import org.pangrova.parser.ast.expression.NotExpression;
import org.pangrova.parser.ast.expression.NotInExpression;
import org.pangrova.parser.ast.expression.UnaryMinusExpression;
import org.pangrova.parser.ast.expression.ArrayConstructor;
import org.pangrova.parser.ast.expression.FunctionCall;
import org.pangrova.parser.ast.expression.MapConstructor;
import org.pangrova.parser.ast.expression.StructConstructor;
import org.pangrova.spi.connector.data.ColumnValue;
import org.pangrova.spi.connector.data.TypedRow;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Evaluates AST expressions against TypedRow and converts literals to ColumnValue.
 */
public final class DmlExpressionEvaluator {

    /** Timestamp formatter that accepts both 'T' and space separators. */
    static final DateTimeFormatter TIMESTAMP_FORMATTER = new DateTimeFormatterBuilder()
            .appendPattern("yyyy-MM-dd")
            .optionalStart().appendLiteral('T').optionalEnd()
            .optionalStart().appendLiteral(' ').optionalEnd()
            .appendPattern("HH:mm:ss")
            .optionalStart().appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true).optionalEnd()
            .toFormatter();

    private DmlExpressionEvaluator() {}

    // ── Boolean evaluation ──

    public static boolean evaluateBooleanExpr(Expression expr, TypedRow row) {
        ColumnValue val = evaluateExpr(expr, row);
        return val instanceof ColumnValue.BooleanValue bv && bv.value();
    }

    // ── General expression evaluation ──

    private static final Map<Class<? extends Expression>, BiFunction<Expression, TypedRow, ColumnValue>>
            EXPR_DISPATCH = Map.of(
            Literal.class, (e, r) -> convertToColumnValue((Literal) e),
            Identifier.class, (e, r) -> resolveIdentifier((Identifier) e, r),
            BinaryExpression.class, (e, r) -> evaluateBinary((BinaryExpression) e, r),
            IsNullExpression.class, (e, r) -> evaluateIsNull((IsNullExpression) e, r),
            NotExpression.class, (e, r) ->
                    new ColumnValue.BooleanValue(!evaluateBooleanExpr(((NotExpression) e).expression(), r)),
            CastExpression.class, (e, r) -> evaluateExpr(((CastExpression) e).operand(), r),
            UnaryMinusExpression.class, (e, r) ->
                    negateColumnValue(evaluateExpr(((UnaryMinusExpression) e).operand(), r)),
            InExpression.class, (e, r) -> evaluateIn((InExpression) e, r, false),
            NotInExpression.class, (e, r) -> evaluateIn(
                    new InExpression(((NotInExpression) e).value(), ((NotInExpression) e).list()), r, true)
    );

    /** Evaluates an expression against a typed row. */
    public static ColumnValue evaluateExpr(Expression expr, TypedRow row) {
        var handler = EXPR_DISPATCH.get(expr.getClass());
        if (handler != null) return handler.apply(expr, row);
        throw new UnsupportedOperationException(
                "Unsupported expression type: " + expr.getClass());
    }

    private static ColumnValue evaluateIsNull(IsNullExpression isNull, TypedRow row) {
        ColumnValue val = evaluateExpr(isNull.operand(), row);
        boolean isNullVal = val instanceof ColumnValue.NullValue;
        return new ColumnValue.BooleanValue(isNull.negated() != isNullVal);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static ColumnValue evaluateIn(InExpression in, TypedRow row, boolean negated) {
        ColumnValue target = evaluateExpr(in.value(), row);
        if (target instanceof ColumnValue.NullValue) {
            return new ColumnValue.BooleanValue(false);
        }
        Comparable targetCmp = toComparable(target);
        for (Expression elem : in.list()) {
            ColumnValue elemVal = evaluateExpr(elem, row);
            if (elemVal instanceof ColumnValue.NullValue) {
                continue;
            }
            if (targetCmp.compareTo(toComparable(elemVal)) == 0) {
                return new ColumnValue.BooleanValue(!negated);
            }
        }
        return new ColumnValue.BooleanValue(negated);
    }

    private static ColumnValue resolveIdentifier(Identifier id, TypedRow row) {
        String qualified = String.join(".", id.parts());
        if (row.containsColumn(qualified)) return row.get(qualified);
        String colName = id.parts().get(id.parts().size() - 1);
        return row.get(colName);
    }

    private static ColumnValue evaluateBinary(BinaryExpression bin, TypedRow row) {
        if (bin.operator() == BinaryExpression.Operator.AND) {
            return new ColumnValue.BooleanValue(
                    evaluateBooleanExpr(bin.left(), row)
                            && evaluateBooleanExpr(bin.right(), row));
        }
        if (bin.operator() == BinaryExpression.Operator.OR) {
            return new ColumnValue.BooleanValue(
                    evaluateBooleanExpr(bin.left(), row)
                            || evaluateBooleanExpr(bin.right(), row));
        }
        ColumnValue left = evaluateExpr(bin.left(), row);
        ColumnValue right = evaluateExpr(bin.right(), row);
        return evaluateComparison(bin.operator(), left, right);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static ColumnValue evaluateComparison(
            BinaryExpression.Operator op, ColumnValue left, ColumnValue right) {
        if (left instanceof ColumnValue.NullValue
                || right instanceof ColumnValue.NullValue) {
            return new ColumnValue.BooleanValue(false);
        }
        Comparable l = toComparable(left);
        Comparable r = toComparable(right);
        int cmp = l.compareTo(r);
        boolean result = switch (op) {
            case EQUAL -> cmp == 0;
            case NOT_EQUAL -> cmp != 0;
            case LESS_THAN -> cmp < 0;
            case GREATER_THAN -> cmp > 0;
            case LESS_THAN_OR_EQUAL -> cmp <= 0;
            case GREATER_THAN_OR_EQUAL -> cmp >= 0;
            default -> throw new UnsupportedOperationException("Unsupported operator: " + op);
        };
        return new ColumnValue.BooleanValue(result);
    }

    @SuppressWarnings("rawtypes")
    private static Comparable toComparable(ColumnValue cv) {
        Object raw = ColumnValue.toRaw(cv);
        Object widened = widenNumeric(raw);
        if (widened instanceof Comparable c) return c;
        throw new IllegalArgumentException("Cannot compare: " + cv.getClass());
    }

    /** Widens Integer→Long and Float→Double for uniform comparison; returns other types unchanged. */
    private static Object widenNumeric(Object raw) {
        if (raw instanceof Integer i) return i.longValue();
        if (raw instanceof Float f) return f.doubleValue();
        if (raw instanceof java.math.BigDecimal bd) return bd.doubleValue();
        return raw;
    }

    // ── Literal extraction and conversion ──

    static ColumnValue extractLiteralColumnValue(Expression expr) {
        if (expr instanceof Literal lit) {
            return convertToColumnValue(lit);
        }
        if (expr instanceof CastExpression cast) {
            return extractCastColumnValue(cast);
        }
        if (expr instanceof UnaryMinusExpression um) {
            return negateColumnValue(extractLiteralColumnValue(um.operand()));
        }
        if (expr instanceof ArrayConstructor arr) {
            return extractArrayColumnValue(arr);
        }
        if (expr instanceof StructConstructor struct) {
            return extractStructColumnValue(struct);
        }
        if (expr instanceof MapConstructor map) {
            return extractMapColumnValue(map);
        }
        if (expr instanceof FunctionCall func) {
            return evaluateInsertFunction(func);
        }
        throw new IllegalArgumentException(
                "Only literal values supported in INSERT: " + expr);
    }

    // ── INSERT function evaluation (FILE_FROM_URL, FILE_FROM_BYTES) ──

    private static ColumnValue evaluateInsertFunction(FunctionCall func) {
        String funcName = String.join(".", func.name().parts()).toUpperCase();
        return switch (funcName) {
            case "FILE_FROM_URL" -> evaluateFileFromUrl(func);
            case "FILE_FROM_BYTES" -> evaluateFileFromBytes(func);
            default -> throw new IllegalArgumentException(
                    "Unsupported function in INSERT VALUES: " + funcName);
        };
    }

    private static final java.util.regex.Pattern URL_SCHEME_PATTERN =
            java.util.regex.Pattern.compile("^([a-zA-Z][a-zA-Z0-9+.-]*)://");

    /**
     * F-INJECT-01: schemes rejected for FILE_FROM_URL because they read server-local or in-process
     * resources (local file disclosure vector). Network/object-store schemes (http/https/s3/gs/...)
     * remain allowed; private-IP / cloud-metadata endpoint and DNS-rebinding checks are enforced at
     * the fetch layer when the URI is actually materialized.
     */
    private static final java.util.Set<String> BLOCKED_FILE_URL_SCHEMES = java.util.Set.of(
            "file", "jar", "jrt", "classpath", "jdbc");

    /**
     * FILE_FROM_URL('uri') → RowValue matching FILE_STRUCT_TYPE:
     * (uri VARCHAR, mime_type VARCHAR, size BIGINT, checksum VARCHAR, created_at TIMESTAMP).
     */
    private static ColumnValue evaluateFileFromUrl(FunctionCall func) {
        if (func.arguments().size() != 1) {
            throw new IllegalArgumentException(
                    "FILE_FROM_URL expects exactly 1 argument, got " + func.arguments().size());
        }
        ColumnValue urlVal = extractLiteralColumnValue(func.arguments().get(0));
        if (!(urlVal instanceof ColumnValue.StringValue sv)) {
            throw new IllegalArgumentException(
                    "FILE_FROM_URL argument must be a string literal");
        }
        String url = sv.value();
        java.util.regex.Matcher schemeMatcher = URL_SCHEME_PATTERN.matcher(url);
        if (!schemeMatcher.find()) {
            throw new IllegalArgumentException(
                    "Invalid URL in FILE_FROM_URL: " + url);
        }
        // F-INJECT-01: reject schemes that read server-local / in-process resources so a SQL-supplied
        // FILE_FROM_URL cannot be used for local file disclosure (e.g. file:///etc/passwd).
        String scheme = schemeMatcher.group(1).toLowerCase(java.util.Locale.ROOT);
        if (BLOCKED_FILE_URL_SCHEMES.contains(scheme)) {
            throw new IllegalArgumentException(
                    "FILE_FROM_URL scheme not allowed (local/in-process resource access): " + scheme);
        }
        // FILE_STRUCT_TYPE: uri, mime_type, size, checksum, created_at
        List<Object> fields = new java.util.ArrayList<>(5);
        fields.add(url);    // uri
        fields.add(null);   // mime_type
        fields.add(null);   // size
        fields.add(null);   // checksum
        fields.add(null);   // created_at
        return new ColumnValue.RowValue(fields);
    }

    /**
     * FILE_FROM_BYTES(X'hex', 'mime_type') → RowValue matching FILE_STRUCT_TYPE.
     */
    private static ColumnValue evaluateFileFromBytes(FunctionCall func) {
        if (func.arguments().size() != 2) {
            throw new IllegalArgumentException(
                    "FILE_FROM_BYTES expects exactly 2 arguments, got " + func.arguments().size());
        }
        ColumnValue bytesVal = extractLiteralColumnValue(func.arguments().get(0));
        ColumnValue mimeVal = extractLiteralColumnValue(func.arguments().get(1));
        long size = 0;
        if (bytesVal instanceof ColumnValue.BytesValue bv) {
            size = bv.value().length;
        }
        String mimeType = (mimeVal instanceof ColumnValue.StringValue sv) ? sv.value() : null;
        // FILE_STRUCT_TYPE: uri, mime_type, size, checksum, created_at
        List<Object> fields = new java.util.ArrayList<>(5);
        fields.add(null);       // uri
        fields.add(mimeType);   // mime_type
        fields.add(size);       // size
        fields.add(null);       // checksum
        fields.add(null);       // created_at
        return new ColumnValue.RowValue(fields);
    }

    private static ColumnValue extractCastColumnValue(CastExpression cast) {
        if (cast.operand() instanceof Literal lit && lit.isNull()) {
            return new ColumnValue.NullValue();
        }
        if (cast.operand() instanceof Literal lit) {
            return convertToColumnValue(lit);
        }
        throw new IllegalArgumentException(
                "Only literal operands supported in CAST: " + cast.operand());
    }

    private static ColumnValue extractArrayColumnValue(ArrayConstructor arr) {
        List<Object> elements = new java.util.ArrayList<>(arr.elements().size());
        for (Expression elem : arr.elements()) {
            ColumnValue cv = extractLiteralColumnValue(elem);
            elements.add(ColumnValue.toRaw(cv));
        }
        return new ColumnValue.ArrayValue(elements);
    }

    private static ColumnValue extractStructColumnValue(StructConstructor struct) {
        List<Object> fields = new java.util.ArrayList<>(struct.fields().size());
        for (StructConstructor.Field field : struct.fields()) {
            ColumnValue cv = extractLiteralColumnValue(field.value());
            fields.add(ColumnValue.toRaw(cv));
        }
        return new ColumnValue.RowValue(fields);
    }

    private static ColumnValue extractMapColumnValue(MapConstructor map) {
        java.util.Map<Object, Object> entries = new java.util.LinkedHashMap<>(map.size());
        for (int i = 0; i < map.size(); i++) {
            ColumnValue keyCv = extractLiteralColumnValue(map.key(i));
            ColumnValue valCv = extractLiteralColumnValue(map.value(i));
            entries.put(ColumnValue.toRaw(keyCv), ColumnValue.toRaw(valCv));
        }
        return new ColumnValue.MapValue(entries);
    }

    private static final Map<Class<? extends LiteralValue>, java.util.function.Function<LiteralValue, ColumnValue>>
            LITERAL_DISPATCH = Map.of(
            LiteralValue.DateValue.class,
                    v -> new ColumnValue.DateValue(LocalDate.parse(((LiteralValue.DateValue) v).value())),
            LiteralValue.TimestampValue.class, v -> new ColumnValue.TimestampValue(
                    LocalDateTime.parse(((LiteralValue.TimestampValue) v).value(), TIMESTAMP_FORMATTER)),
            LiteralValue.IntegerValue.class, v -> new ColumnValue.LongValue(((LiteralValue.IntegerValue) v).value()),
            LiteralValue.DecimalValue.class, v -> new ColumnValue.DecimalValue(
                    ((LiteralValue.DecimalValue) v).value()),
            LiteralValue.StringValue.class, v -> new ColumnValue.StringValue(((LiteralValue.StringValue) v).value()),
            LiteralValue.UuidValue.class, v -> new ColumnValue.UuidValue(((LiteralValue.UuidValue) v).value()),
            LiteralValue.BooleanValue.class, v -> new ColumnValue.BooleanValue(((LiteralValue.BooleanValue) v).value()),
            LiteralValue.NullValue.class, v -> new ColumnValue.NullValue(),
            LiteralValue.BinaryHexValue.class, v -> new ColumnValue.BytesValue(
                    hexToBytes(((LiteralValue.BinaryHexValue) v).hexDigits()))
    );

    static ColumnValue convertToColumnValue(Literal lit) {
        var handler = LITERAL_DISPATCH.get(lit.value().getClass());
        if (handler != null) return handler.apply(lit.value());
        throw new IllegalArgumentException("Unknown literal type: " + lit.value().getClass());
    }

    /** Converts a Literal to raw Object for framework boundaries (Calcite interop). */
    public static Object convertLiteralValue(Literal lit) {
        return ColumnValue.toRaw(convertToColumnValue(lit));
    }

    public static Map<String, String> convertProperties(Map<String, Literal> props) {
        Map<String, String> result = new LinkedHashMap<>();
        props.forEach((k, v) -> result.put(k, String.valueOf(convertLiteralValue(v))));
        return result;
    }

    private static final Map<Class<? extends ColumnValue>, java.util.function.Function<ColumnValue, ColumnValue>>
            NEGATE_DISPATCH = Map.of(
            ColumnValue.LongValue.class, v -> new ColumnValue.LongValue(-((ColumnValue.LongValue) v).value()),
            ColumnValue.IntValue.class, v -> new ColumnValue.IntValue(-((ColumnValue.IntValue) v).value()),
            ColumnValue.DoubleValue.class, v -> new ColumnValue.DoubleValue(-((ColumnValue.DoubleValue) v).value()),
            ColumnValue.FloatValue.class, v -> new ColumnValue.FloatValue(-((ColumnValue.FloatValue) v).value()),
            ColumnValue.DecimalValue.class,
                    v -> new ColumnValue.DecimalValue(((ColumnValue.DecimalValue) v).value().negate())
    );

    private static byte[] hexToBytes(String hex) {
        int len = hex.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(hex.charAt(i), 16) << 4)
                    + Character.digit(hex.charAt(i + 1), 16));
        }
        return data;
    }

    static ColumnValue negateColumnValue(ColumnValue cv) {
        var handler = NEGATE_DISPATCH.get(cv.getClass());
        if (handler != null) {
            return handler.apply(cv);
        }
        throw new IllegalArgumentException("Cannot negate non-numeric ColumnValue: " + cv.getClass());
    }
}

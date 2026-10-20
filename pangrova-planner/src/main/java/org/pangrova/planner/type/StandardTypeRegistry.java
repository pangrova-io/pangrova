package org.pangrova.planner.type;

import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.spi.connector.data.type.ArrayType;
import org.pangrova.spi.connector.data.type.MapType;
import org.pangrova.spi.connector.data.type.RowField;
import org.pangrova.spi.connector.data.type.RowType;
import org.pangrova.spi.connector.data.type.Type;
import org.pangrova.spi.connector.data.type.TypeManager;
import org.pangrova.spi.connector.data.type.TypeSignature;
import org.pangrova.spi.connector.data.type.primitive.BigIntType;
import org.pangrova.spi.connector.data.type.primitive.BooleanType;
import org.pangrova.spi.connector.data.type.primitive.CharType;
import org.pangrova.spi.connector.data.type.primitive.DecimalType;
import org.pangrova.spi.connector.data.type.primitive.DoubleType;
import org.pangrova.spi.connector.data.type.primitive.IntegerType;
import org.pangrova.spi.connector.data.type.primitive.RealType;
import org.pangrova.spi.connector.data.type.primitive.SmallIntType;
import org.pangrova.spi.connector.data.type.primitive.TinyIntType;
import org.pangrova.spi.connector.data.type.primitive.UuidType;
import org.pangrova.spi.connector.data.type.primitive.VarbinaryType;
import org.pangrova.spi.connector.data.type.primitive.VarcharType;
import org.pangrova.spi.connector.data.type.primitive.temporal.DateType;
import org.pangrova.spi.connector.data.type.primitive.temporal.IntervalDayTimeType;
import org.pangrova.spi.connector.data.type.primitive.temporal.IntervalYearMonthType;
import org.pangrova.spi.connector.data.type.primitive.temporal.TimeType;
import org.pangrova.spi.connector.data.type.primitive.temporal.TimestampType;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Standard {@link TypeManager} with pre-registered base types,
 * numeric widening compatibility, and common super-type derivation.
 *
 * <p>Concurrency strategy: uses {@link ConcurrentHashMap} for the type registry.
 * The registry is populated at construction time with all base types and is
 * effectively immutable after construction (no public mutation methods).</p>
 */
@ThreadSafe
public final class StandardTypeRegistry implements TypeManager {

    private final ConcurrentHashMap<String, Type> registry = new ConcurrentHashMap<>();

    /**
     * Numeric type widening order (index = widening rank).
     * TINYINT(0) → SMALLINT(1) → INTEGER(2) → BIGINT(3) → REAL(4) → DOUBLE(5).
     */
    private static final Map<Class<? extends Type>, Integer> NUMERIC_RANK = Map.of(
            TinyIntType.class, 0,
            SmallIntType.class, 1,
            IntegerType.class, 2,
            BigIntType.class, 3,
            RealType.class, 4,
            DoubleType.class, 5
    );

    private static final Type[] NUMERIC_BY_RANK = {
            TinyIntType.INSTANCE,
            SmallIntType.INSTANCE,
            IntegerType.INSTANCE,
            BigIntType.INSTANCE,
            RealType.INSTANCE,
            DoubleType.INSTANCE
    };

    public StandardTypeRegistry() {
        registerBaseTypes();
    }

    @Override
    public Type getType(TypeSignature signature) {
        Type cached = registry.get(signature.base());
        if (cached != null && signature.parameters().isEmpty()) {
            return cached;
        }
        return resolveParameterized(signature);
    }

    @Override
    public boolean isCompatible(Type source, Type target) {
        if (source.equals(target)) {
            return true;
        }
        return isNumericWidening(source, target);
    }

    @Override
    public Optional<Type> getCommonSuperType(Type a, Type b) {
        if (a.equals(b)) {
            return Optional.of(a);
        }
        return deriveNumericSuperType(a, b);
    }

    private boolean isNumericWidening(Type source, Type target) {
        Integer sourceRank = NUMERIC_RANK.get(source.getClass());
        Integer targetRank = NUMERIC_RANK.get(target.getClass());
        if (sourceRank == null || targetRank == null) {
            return false;
        }
        return sourceRank <= targetRank;
    }

    private Optional<Type> deriveNumericSuperType(Type a, Type b) {
        Integer rankA = NUMERIC_RANK.get(a.getClass());
        Integer rankB = NUMERIC_RANK.get(b.getClass());
        if (rankA == null || rankB == null) {
            return Optional.empty();
        }
        int maxRank = Math.max(rankA, rankB);
        return Optional.of(NUMERIC_BY_RANK[maxRank]);
    }

    private Type resolveParameterized(TypeSignature signature) {
        return switch (signature.base()) {
            case "DECIMAL" -> resolveDecimal(signature);
            case "VARCHAR" -> resolveVarchar(signature);
            case "CHAR" -> resolveChar(signature);
            case "ARRAY" -> resolveArray(signature);
            case "MAP" -> resolveMap(signature);
            case "ROW" -> resolveRow(signature);
            default -> resolveSimple(signature);
        };
    }

    private Type resolveDecimal(TypeSignature sig) {
        var params = sig.parameters();
        int precision = params.get(0).longValue().intValue();
        int scale = params.get(1).longValue().intValue();
        return new DecimalType(precision, scale);
    }

    private Type resolveVarchar(TypeSignature sig) {
        if (sig.parameters().isEmpty()) {
            return VarcharType.UNBOUNDED;
        }
        int length = sig.parameters().get(0).longValue().intValue();
        return new VarcharType(length);
    }

    private Type resolveChar(TypeSignature sig) {
        int length = sig.parameters().get(0).longValue().intValue();
        return new CharType(length);
    }

    private Type resolveArray(TypeSignature sig) {
        Type elementType = getType(sig.parameters().get(0).typeSignature());
        return new ArrayType(elementType);
    }

    private Type resolveMap(TypeSignature sig) {
        Type keyType = getType(sig.parameters().get(0).typeSignature());
        Type valueType = getType(sig.parameters().get(1).typeSignature());
        return new MapType(keyType, valueType);
    }

    private Type resolveRow(TypeSignature sig) {
        var fields = sig.parameters().stream()
                .map(p -> new RowField(p.fieldName(), getType(p.typeSignature())))
                .toList();
        return new RowType(fields);
    }

    private Type resolveSimple(TypeSignature sig) {
        Type type = registry.get(sig.base());
        if (type == null) {
            throw new IllegalArgumentException(
                    "Unknown type signature: " + sig.base());
        }
        return type;
    }

    private void registerBaseTypes() {
        registry.put("BOOLEAN", BooleanType.INSTANCE);
        registry.put("TINYINT", TinyIntType.INSTANCE);
        registry.put("SMALLINT", SmallIntType.INSTANCE);
        registry.put("INTEGER", IntegerType.INSTANCE);
        registry.put("BIGINT", BigIntType.INSTANCE);
        registry.put("REAL", RealType.INSTANCE);
        registry.put("DOUBLE", DoubleType.INSTANCE);
        registry.put("DATE", DateType.INSTANCE);
        registry.put("TIME", TimeType.INSTANCE);
        registry.put("TIMESTAMP", TimestampType.WITHOUT_TIMEZONE);
        registry.put("TIMESTAMP_TZ", TimestampType.WITH_TIMEZONE);
        registry.put("VARCHAR", VarcharType.UNBOUNDED);
        registry.put("VARBINARY", VarbinaryType.INSTANCE);
        registry.put("UUID", UuidType.INSTANCE);
        registry.put("INTERVAL_YEAR_MONTH", IntervalYearMonthType.INSTANCE);
        registry.put("INTERVAL_DAY_TIME", IntervalDayTimeType.INSTANCE);
    }
}

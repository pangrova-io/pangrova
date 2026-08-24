package org.pangrova.spi.property.connector.data;

import java.util.List;
import java.util.stream.Collectors;

import org.pangrova.spi.connector.data.type.ArrayType;
import org.pangrova.spi.connector.data.type.primitive.BigIntType;
import org.pangrova.spi.connector.data.type.MapType;
import org.pangrova.spi.connector.data.type.RowField;
import org.pangrova.spi.connector.data.type.RowType;
import org.pangrova.spi.connector.data.type.primitive.SmallIntType;
import org.pangrova.spi.connector.data.type.Type;
import org.pangrova.spi.connector.data.type.TypeSignature;
import org.pangrova.spi.connector.data.type.primitive.BooleanType;
import org.pangrova.spi.connector.data.type.primitive.CharType;
import org.pangrova.spi.connector.data.type.primitive.temporal.DateType;
import org.pangrova.spi.connector.data.type.primitive.DecimalType;
import org.pangrova.spi.connector.data.type.primitive.DoubleType;
import org.pangrova.spi.connector.data.type.primitive.IntegerType;
import org.pangrova.spi.connector.data.type.primitive.RealType;
import org.pangrova.spi.connector.data.type.primitive.TinyIntType;
import org.pangrova.spi.connector.data.type.primitive.VarbinaryType;
import org.pangrova.spi.connector.data.type.primitive.VarcharType;
import org.pangrova.spi.connector.data.type.primitive.temporal.TimeType;
import org.pangrova.spi.connector.data.type.primitive.temporal.TimestampType;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property-based tests for the parameterized Type system.
 *
 * <p><b>Validates: Requirements 5.1, 5.2, 5.5</b></p>
 */
class TypeRoundTripPropertyTest {

    /**
     * Property 6: Type Round-Trip
     *
     * For any Type instance, serializing to TypeSignature and deserializing
     * back should produce an equals-equal Type instance.
     *
     * <p><b>Validates: Requirements 5.1, 5.2, 5.5</b></p>
     */
    @Property(tries = 10)
    void typeRoundTripPreservesEquality(@ForAll("types") Type original) {
        TypeSignature signature = original.getTypeSignature();
        Type restored = fromSignature(signature);

        assertThat(restored)
                .as("Round-trip for %s", original.getDisplayName())
                .isEqualTo(original);
    }

    /**
     * For any Type, getDisplayName() is non-null and non-empty.
     */
    @Property(tries = 10)
    void displayNameIsNonEmpty(@ForAll("types") Type type) {
        assertThat(type.getDisplayName()).isNotNull().isNotEmpty();
    }

    /**
     * For any Type, getTypeSignature() is non-null with a non-empty base.
     */
    @Property(tries = 10)
    void typeSignatureHasNonEmptyBase(@ForAll("types") Type type) {
        TypeSignature sig = type.getTypeSignature();
        assertThat(sig).isNotNull();
        assertThat(sig.base()).isNotNull().isNotEmpty();
    }

    /**
     * For any Type, serializing to signature twice yields equal signatures.
     */
    @Property(tries = 10)
    void typeSignatureIsDeterministic(@ForAll("types") Type type) {
        assertThat(type.getTypeSignature()).isEqualTo(type.getTypeSignature());
    }

    // ---- Arbitrary generators ----

    @Provide
    Arbitrary<Type> types() {
        return Arbitraries.lazyOf(
                this::simpleTypes,
                this::parameterizedTypes,
                this::complexTypes);
    }

    private Arbitrary<Type> simpleTypes() {
        return Arbitraries.of(
                BooleanType.INSTANCE,
                TinyIntType.INSTANCE,
                SmallIntType.INSTANCE,
                IntegerType.INSTANCE,
                BigIntType.INSTANCE,
                RealType.INSTANCE,
                DoubleType.INSTANCE,
                VarbinaryType.INSTANCE,
                DateType.INSTANCE,
                TimeType.INSTANCE,
                TimestampType.WITHOUT_TIMEZONE,
                TimestampType.WITH_TIMEZONE);
    }

    private Arbitrary<Type> parameterizedTypes() {
        Arbitrary<Type> decimals = Arbitraries.integers().between(1, 38)
                .flatMap(p -> Arbitraries.integers().between(0, p)
                        .map(s -> new DecimalType(p, s)));

        Arbitrary<Type> varchars = Arbitraries.integers().between(1, 65535)
                .map(VarcharType::new);

        Arbitrary<Type> chars = Arbitraries.integers().between(1, 255)
                .map(CharType::new);

        return Arbitraries.oneOf(decimals, varchars, chars);
    }

    private Arbitrary<Type> complexTypes() {
        Arbitrary<Type> leafTypes = Arbitraries.oneOf(simpleTypes(), parameterizedTypes());

        Arbitrary<Type> arrays = leafTypes.map(ArrayType::new);

        Arbitrary<Type> maps = leafTypes.flatMap(k -> leafTypes.map(v -> new MapType(k, v)));

        Arbitrary<Type> rows = leafTypes.flatMap(t1 ->
                leafTypes.map(t2 -> new RowType(List.of(
                        new RowField("f1", t1),
                        new RowField("f2", t2)))));

        return Arbitraries.oneOf(arrays, maps, rows);
    }

    // ---- Minimal TypeSignature → Type resolver for round-trip testing ----

    private static Type fromSignature(TypeSignature signature) {
        return switch (signature.base()) {
            case "BOOLEAN" -> BooleanType.INSTANCE;
            case "TINYINT" -> TinyIntType.INSTANCE;
            case "SMALLINT" -> SmallIntType.INSTANCE;
            case "INTEGER" -> IntegerType.INSTANCE;
            case "BIGINT" -> BigIntType.INSTANCE;
            case "REAL" -> RealType.INSTANCE;
            case "DOUBLE" -> DoubleType.INSTANCE;
            case "VARBINARY" -> VarbinaryType.INSTANCE;
            case "DATE" -> DateType.INSTANCE;
            case "TIME" -> TimeType.INSTANCE;
            case "TIMESTAMP" -> TimestampType.WITHOUT_TIMEZONE;
            case "TIMESTAMP_TZ" -> TimestampType.WITH_TIMEZONE;
            case "DECIMAL" -> {
                long precision = signature.parameters().get(0).longValue();
                long scale = signature.parameters().get(1).longValue();
                yield new DecimalType((int) precision, (int) scale);
            }
            case "VARCHAR" -> {
                long length = signature.parameters().get(0).longValue();
                yield new VarcharType((int) length);
            }
            case "CHAR" -> {
                long length = signature.parameters().get(0).longValue();
                yield new CharType((int) length);
            }
            case "ARRAY" -> {
                Type elementType = fromSignature(
                        signature.parameters().get(0).typeSignature());
                yield new ArrayType(elementType);
            }
            case "MAP" -> {
                Type keyType = fromSignature(
                        signature.parameters().get(0).typeSignature());
                Type valueType = fromSignature(
                        signature.parameters().get(1).typeSignature());
                yield new MapType(keyType, valueType);
            }
            case "ROW" -> {
                List<RowField> fields = signature.parameters().stream()
                        .map(p -> new RowField(
                                p.fieldName(),
                                fromSignature(p.typeSignature())))
                        .collect(Collectors.toList());
                yield new RowType(fields);
            }
            default -> throw new IllegalArgumentException(
                    "Unknown type signature: " + signature.base());
        };
    }
}

package org.pangrova.planner.property.type;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.constraints.IntRange;

import java.util.Optional;

import org.pangrova.planner.type.StandardTypeRegistry;
import org.pangrova.spi.connector.data.type.PangrovaType;
import org.pangrova.spi.connector.data.type.mapping.PangrovaTypeMapping;
import org.pangrova.spi.connector.data.type.Type;
import org.pangrova.spi.connector.data.type.TypeSignature;
import org.pangrova.spi.connector.data.type.primitive.BigIntType;
import org.pangrova.spi.connector.data.type.primitive.BooleanType;
import org.pangrova.spi.connector.data.type.primitive.DecimalType;
import org.pangrova.spi.connector.data.type.primitive.DoubleType;
import org.pangrova.spi.connector.data.type.primitive.IntegerType;
import org.pangrova.spi.connector.data.type.primitive.RealType;
import org.pangrova.spi.connector.data.type.primitive.SmallIntType;
import org.pangrova.spi.connector.data.type.primitive.TinyIntType;
import org.pangrova.spi.connector.data.type.primitive.VarbinaryType;
import org.pangrova.spi.connector.data.type.primitive.VarcharType;
import org.pangrova.spi.connector.data.type.primitive.temporal.DateType;
import org.pangrova.spi.connector.data.type.primitive.temporal.TimeType;
import org.pangrova.spi.connector.data.type.primitive.temporal.TimestampType;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Property-based tests for the parameterized type system.
 */
class TypeSystemPropertyTest {

    private final StandardTypeRegistry typeRegistry = new StandardTypeRegistry();

    private static final PangrovaType[] ROUND_TRIP_TYPES = {
            PangrovaType.BOOLEAN,
            PangrovaType.TINYINT,
            PangrovaType.SMALLINT,
            PangrovaType.INTEGER,
            PangrovaType.BIGINT,
            PangrovaType.FLOAT,
            PangrovaType.DOUBLE,
            PangrovaType.DECIMAL,
            PangrovaType.DATE,
            PangrovaType.TIME,
            PangrovaType.TIMESTAMP,
            PangrovaType.TIMESTAMP_TZ,
            PangrovaType.VARCHAR,
            PangrovaType.VARBINARY
    };

    @Provide
    Arbitrary<PangrovaType> roundTripPangrovaTypes() {
        return Arbitraries.of(ROUND_TRIP_TYPES);
    }

    @Property(tries = 10)
    void bridgeRoundTripPreservesIdentity(
            @ForAll("roundTripPangrovaTypes") PangrovaType original) {
        Type converted = PangrovaTypeMapping.toType(original);
        PangrovaType roundTripped = PangrovaTypeMapping.toPangrovaType(converted);
        assertEquals(original, roundTripped,
                "Round-trip failed for " + original);
    }

    @Provide
    Arbitrary<Type> baseTypes() {
        return Arbitraries.of(
                BooleanType.INSTANCE,
                TinyIntType.INSTANCE,
                SmallIntType.INSTANCE,
                IntegerType.INSTANCE,
                BigIntType.INSTANCE,
                RealType.INSTANCE,
                DoubleType.INSTANCE,
                DateType.INSTANCE,
                TimeType.INSTANCE,
                TimestampType.WITHOUT_TIMEZONE,
                TimestampType.WITH_TIMEZONE,
                VarcharType.UNBOUNDED,
                VarbinaryType.INSTANCE
        );
    }

    @Property(tries = 10)
    void typeRegistryResolvesAllRegisteredTypes(
            @ForAll("baseTypes") Type original) {
        TypeSignature sig = original.getTypeSignature();
        Type resolved = typeRegistry.getType(sig);
        assertNotNull(resolved);
        assertEquals(original, resolved,
                "Resolution failed for " + sig.base());
    }

    @Provide
    Arbitrary<Type> numericTypes() {
        return Arbitraries.of(
                TinyIntType.INSTANCE,
                SmallIntType.INSTANCE,
                IntegerType.INSTANCE,
                BigIntType.INSTANCE,
                RealType.INSTANCE,
                DoubleType.INSTANCE
        );
    }

    @Property(tries = 10)
    void numericWideningIsTransitive(
            @ForAll("numericTypes") Type source,
            @ForAll("numericTypes") Type target) {
        boolean compatible = typeRegistry.isCompatible(source, target);
        int sourceRank = numericRank(source);
        int targetRank = numericRank(target);
        assertEquals(sourceRank <= targetRank, compatible,
                source.getDisplayName() + " → " + target.getDisplayName());
    }

    @Property(tries = 10)
    void commonSuperTypeIsSymmetric(
            @ForAll("numericTypes") Type a,
            @ForAll("numericTypes") Type b) {
        Optional<Type> ab = typeRegistry.getCommonSuperType(a, b);
        Optional<Type> ba = typeRegistry.getCommonSuperType(b, a);
        assertEquals(ab, ba,
                "Symmetry violated for " + a.getDisplayName()
                        + " and " + b.getDisplayName());
    }

    @Property(tries = 10)
    void decimalSignatureRoundTrip(
            @ForAll @IntRange(min = 1, max = 38) int precision,
            @ForAll @IntRange(min = 0, max = 38) int scale) {
        if (scale > precision) {
            return;
        }
        DecimalType original = new DecimalType(precision, scale);
        TypeSignature sig = original.getTypeSignature();
        Type resolved = typeRegistry.getType(sig);
        assertEquals(original, resolved);
    }

    private static int numericRank(Type type) {
        if (type instanceof TinyIntType) return 0;
        if (type instanceof SmallIntType) return 1;
        if (type instanceof IntegerType) return 2;
        if (type instanceof BigIntType) return 3;
        if (type instanceof RealType) return 4;
        if (type instanceof DoubleType) return 5;
        return -1;
    }
}

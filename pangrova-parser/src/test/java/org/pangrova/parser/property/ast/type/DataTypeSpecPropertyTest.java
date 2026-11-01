package org.pangrova.parser.property.ast.type;

import net.jqwik.api.*;
import net.jqwik.api.arbitraries.IntegerArbitrary;
import org.pangrova.parser.ast.type.*;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property 4: DataTypeSpec parse round-trip correctness.
 *
 * <p><b>Validates: Requirements 3.2</b></p>
 *
 * <p>For any valid DataTypeSpec instance (including nested MAP/STRUCT/ARRAY types),
 * {@code DataTypeSpec.fromRawTypeString(spec.toCanonicalString())} must produce
 * a structurally equal DataTypeSpec.</p>
 */
@Tag("production-readiness-fixes")
@Tag("Property4-DataTypeSpec-round-trip")
class DataTypeSpecPropertyTest {

    private static final int MAX_DEPTH = 3;

    @Property(tries = 200)
    void roundTripPreservesStructuralEquality(@ForAll("dataTypeSpecs") DataTypeSpec original) {
        String canonical = original.toCanonicalString();
        DataTypeSpec reparsed = DataTypeSpec.fromRawTypeString(canonical);

        assertThat(reparsed)
                .as("Round-trip for canonical string [%s]", canonical)
                .isEqualTo(original);
    }

    @Property(tries = 200)
    void canonicalStringIsIdempotent(@ForAll("dataTypeSpecs") DataTypeSpec original) {
        String first = original.toCanonicalString();
        DataTypeSpec reparsed = DataTypeSpec.fromRawTypeString(first);
        String second = reparsed.toCanonicalString();

        assertThat(second)
                .as("Canonical string should be stable across round-trips")
                .isEqualTo(first);
    }

    @Provide
    Arbitrary<DataTypeSpec> dataTypeSpecs() {
        return dataTypeSpecAtDepth(0);
    }

    private Arbitrary<DataTypeSpec> dataTypeSpecAtDepth(int depth) {
        Arbitrary<DataTypeSpec> primitive = primitiveTypes();
        Arbitrary<DataTypeSpec> decimal = decimalTypes();
        Arbitrary<DataTypeSpec> vector = vectorTypes();

        if (depth >= MAX_DEPTH) {
            return Arbitraries.oneOf(primitive, decimal, vector);
        }

        Arbitrary<DataTypeSpec> array = arrayTypes(depth);
        Arbitrary<DataTypeSpec> map = mapTypes(depth);
        Arbitrary<DataTypeSpec> struct = structTypes(depth);

        return Arbitraries.oneOf(primitive, decimal, vector, array, map, struct);
    }

    private Arbitrary<DataTypeSpec> primitiveTypes() {
        return Arbitraries.of(BaseType.values())
                .map(DataTypeSpec.PrimitiveType::new);
    }

    private Arbitrary<DataTypeSpec> decimalTypes() {
        IntegerArbitrary precisions = Arbitraries.integers().between(1, 38);
        return precisions.flatMap(precision ->
                Arbitraries.integers().between(0, precision)
                        .map(scale -> new DataTypeSpec.DecimalType(precision, scale))
        );
    }

    private Arbitrary<DataTypeSpec> vectorTypes() {
        Arbitrary<Integer> dimensions = Arbitraries.integers().between(1, 4096);
        Arbitrary<DistanceMetric> metrics = Arbitraries.of(DistanceMetric.values());
        var elements = Arbitraries.of(org.pangrova.spi.connector.data.type.VectorType.ElementType.values());
        return Combinators.combine(dimensions, metrics, elements)
                .as(DataTypeSpec.VectorType::new);
    }

    private Arbitrary<DataTypeSpec> arrayTypes(int depth) {
        return dataTypeSpecAtDepth(depth + 1)
                .map(DataTypeSpec.ArrayType::new);
    }

    private Arbitrary<DataTypeSpec> mapTypes(int depth) {
        return Combinators.combine(
                dataTypeSpecAtDepth(depth + 1),
                dataTypeSpecAtDepth(depth + 1)
        ).as(DataTypeSpec.MapType::new);
    }

    private Arbitrary<DataTypeSpec> structTypes(int depth) {
        return Arbitraries.integers().between(1, 10).flatMap(fieldCount ->
                fieldSpecList(depth, fieldCount).map(DataTypeSpec.StructType::new)
        );
    }

    private Arbitrary<List<FieldSpec>> fieldSpecList(int depth, int count) {
        return Arbitraries.just(count).flatMap(n -> {
            List<Arbitrary<FieldSpec>> fieldArbitraries = IntStream.range(0, n)
                    .mapToObj(i -> fieldSpec(depth, i))
                    .collect(Collectors.toList());
            return Combinators.combine(fieldArbitraries).as(fields -> fields);
        });
    }

    private Arbitrary<FieldSpec> fieldSpec(int depth, int index) {
        Arbitrary<String> names = Arbitraries.of(
                "f" + index, "col" + index, "field" + index,
                "a" + index, "x" + index
        );
        return Combinators.combine(names, dataTypeSpecAtDepth(depth + 1))
                .as(FieldSpec::new);
    }
}

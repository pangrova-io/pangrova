package org.pangrova.worker.function;

import org.pangrova.worker.enums.NativeFunctionType;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link NativeFunctionDescriptor}.
 */
class NativeFunctionDescriptorTest {

    @Test
    void canonicalSerializationPreservesBothDeterminismValues()
            throws com.google.protobuf.InvalidProtocolBufferException {
        for (boolean deterministic : List.of(false, true)) {
            var descriptor = new NativeFunctionDescriptor(
                    "file_from_url", "ROW", List.of("VARCHAR"),
                    NativeFunctionType.SCALAR, deterministic);
            var canonical = org.pangrova.protocol.v2.NativeFunctionDescriptor.parseFrom(descriptor.toBytes());
            assertThat(canonical.getName()).isEqualTo(descriptor.name());
            assertThat(canonical.getReturnType().getCanonicalSql()).isEqualTo(descriptor.returnType());
            assertThat(canonical.getArgumentTypesList())
                    .extracting(org.pangrova.protocol.v2.LogicalType::getCanonicalSql)
                    .containsExactly("VARCHAR");
            assertThat(canonical.getFunctionKind()).isEqualTo(
                    org.pangrova.protocol.v2.NativeFunctionKind.NATIVE_FUNCTION_KIND_SCALAR);
            assertThat(canonical.getDeterministic()).isEqualTo(deterministic);
        }
    }

    @Test
    void explicitNullInputPolicyRetainsPresenceForBothBooleanValues() throws Exception {
        for (boolean strict : List.of(false, true)) {
            var descriptor = new NativeFunctionDescriptor("file_from_url", "ROW", List.of("VARCHAR"),
                    NativeFunctionType.SCALAR, false, strict);
            var canonical = org.pangrova.protocol.v2.NativeFunctionDescriptor.parseFrom(descriptor.toBytes());
            assertThat(canonical.hasDefaultNullBehavior()).isTrue();
            assertThat(canonical.getDefaultNullBehavior()).isEqualTo(strict);
        }
    }

    @Test
    void scalarDescriptorPreservesFields() {
        var desc = new NativeFunctionDescriptor(
                "my_add", "BIGINT", List.of("BIGINT", "BIGINT"),
                NativeFunctionType.SCALAR, true);

        assertThat(desc.name()).isEqualTo("my_add");
        assertThat(desc.returnType()).isEqualTo("BIGINT");
        assertThat(desc.argumentTypes()).containsExactly("BIGINT", "BIGINT");
        assertThat(desc.functionType()).isEqualTo(NativeFunctionType.SCALAR);
        assertThat(desc.deterministic()).isTrue();
        assertThat(desc.defaultNullBehavior()).isTrue();
    }

    @Test
    void aggregateDescriptorPreservesFields() {
        var desc = new NativeFunctionDescriptor(
                "my_sum", "DOUBLE", List.of("DOUBLE"),
                NativeFunctionType.AGGREGATE, false);

        assertThat(desc.functionType()).isEqualTo(NativeFunctionType.AGGREGATE);
        assertThat(desc.deterministic()).isFalse();
    }

    @Test
    void nullArgumentTypesDefaultsToEmptyList() {
        var desc = new NativeFunctionDescriptor(
                "no_args", "VARCHAR", null,
                NativeFunctionType.SCALAR, true);

        assertThat(desc.argumentTypes()).isEmpty();
    }

    @Test
    void argumentTypesAreDefensivelyCopied() {
        var mutableList = new java.util.ArrayList<>(List.of("INT", "VARCHAR"));
        var desc = new NativeFunctionDescriptor(
                "fn", "INT", mutableList,
                NativeFunctionType.SCALAR, true);

        mutableList.add("DOUBLE");
        assertThat(desc.argumentTypes()).hasSize(2);
    }

    @Test
    void nullNameThrows() {
        assertThatThrownBy(() -> new NativeFunctionDescriptor(
                null, "INT", List.of(),
                NativeFunctionType.SCALAR, true))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("name");
    }

    @Test
    void nullReturnTypeThrows() {
        assertThatThrownBy(() -> new NativeFunctionDescriptor(
                "fn", null, List.of(),
                NativeFunctionType.SCALAR, true))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("returnType");
    }

    @Test
    void nullFunctionTypeThrows() {
        assertThatThrownBy(() -> new NativeFunctionDescriptor(
                "fn", "INT", List.of(), null, true))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("functionType");
    }

    @Test
    void toBytesProducesNonEmptyArray() {
        var desc = new NativeFunctionDescriptor(
                "test_fn", "VARCHAR", List.of("INT", "DOUBLE"),
                NativeFunctionType.SCALAR, true);

        byte[] bytes = desc.toBytes();
        assertThat(bytes).isNotNull().isNotEmpty();
        // Verify the bytes contain the function name as UTF-8 substring
        String asString = new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
        assertThat(asString).contains("test_fn");
    }

    @Test
    void toBytesWithNoArgsProducesNonEmptyArray() {
        var desc = new NativeFunctionDescriptor(
                "zero_arg_fn", "BOOLEAN", List.of(),
                NativeFunctionType.AGGREGATE, false);

        byte[] bytes = desc.toBytes();
        assertThat(bytes).isNotNull().isNotEmpty();
    }

    @Test
    void functionTypeOrdinals() {
        assertThat(NativeFunctionType.SCALAR.ordinal()).isEqualTo(0);
        assertThat(NativeFunctionType.AGGREGATE.ordinal()).isEqualTo(1);
    }

    @Test
    void equalDescriptorsAreEqual() {
        var desc1 = new NativeFunctionDescriptor(
                "fn", "INT", List.of("INT"),
                NativeFunctionType.SCALAR, true);
        var desc2 = new NativeFunctionDescriptor(
                "fn", "INT", List.of("INT"),
                NativeFunctionType.SCALAR, true);

        assertThat(desc1).isEqualTo(desc2);
        assertThat(desc1.hashCode()).isEqualTo(desc2.hashCode());
    }
}

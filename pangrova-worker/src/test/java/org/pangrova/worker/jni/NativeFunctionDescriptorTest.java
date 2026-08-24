package org.pangrova.worker.jni;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link NativeFunctionDescriptor}.
 */
class NativeFunctionDescriptorTest {

    @Test
    void scalarDescriptorPreservesFields() {
        var desc = new NativeFunctionDescriptor(
                "my_add", "BIGINT", List.of("BIGINT", "BIGINT"),
                NativeFunctionDescriptor.FunctionType.SCALAR, true);

        assertThat(desc.name()).isEqualTo("my_add");
        assertThat(desc.returnType()).isEqualTo("BIGINT");
        assertThat(desc.argumentTypes()).containsExactly("BIGINT", "BIGINT");
        assertThat(desc.functionType()).isEqualTo(NativeFunctionDescriptor.FunctionType.SCALAR);
        assertThat(desc.deterministic()).isTrue();
    }

    @Test
    void aggregateDescriptorPreservesFields() {
        var desc = new NativeFunctionDescriptor(
                "my_sum", "DOUBLE", List.of("DOUBLE"),
                NativeFunctionDescriptor.FunctionType.AGGREGATE, false);

        assertThat(desc.functionType()).isEqualTo(NativeFunctionDescriptor.FunctionType.AGGREGATE);
        assertThat(desc.deterministic()).isFalse();
    }

    @Test
    void nullArgumentTypesDefaultsToEmptyList() {
        var desc = new NativeFunctionDescriptor(
                "no_args", "VARCHAR", null,
                NativeFunctionDescriptor.FunctionType.SCALAR, true);

        assertThat(desc.argumentTypes()).isEmpty();
    }

    @Test
    void argumentTypesAreDefensivelyCopied() {
        var mutableList = new java.util.ArrayList<>(List.of("INT", "VARCHAR"));
        var desc = new NativeFunctionDescriptor(
                "fn", "INT", mutableList,
                NativeFunctionDescriptor.FunctionType.SCALAR, true);

        mutableList.add("DOUBLE");
        assertThat(desc.argumentTypes()).hasSize(2);
    }

    @Test
    void nullNameThrows() {
        assertThatThrownBy(() -> new NativeFunctionDescriptor(
                null, "INT", List.of(),
                NativeFunctionDescriptor.FunctionType.SCALAR, true))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("name");
    }

    @Test
    void nullReturnTypeThrows() {
        assertThatThrownBy(() -> new NativeFunctionDescriptor(
                "fn", null, List.of(),
                NativeFunctionDescriptor.FunctionType.SCALAR, true))
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
                NativeFunctionDescriptor.FunctionType.SCALAR, true);

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
                NativeFunctionDescriptor.FunctionType.AGGREGATE, false);

        byte[] bytes = desc.toBytes();
        assertThat(bytes).isNotNull().isNotEmpty();
    }

    @Test
    void functionTypeOrdinals() {
        assertThat(NativeFunctionDescriptor.FunctionType.SCALAR.ordinal()).isEqualTo(0);
        assertThat(NativeFunctionDescriptor.FunctionType.AGGREGATE.ordinal()).isEqualTo(1);
    }

    @Test
    void equalDescriptorsAreEqual() {
        var desc1 = new NativeFunctionDescriptor(
                "fn", "INT", List.of("INT"),
                NativeFunctionDescriptor.FunctionType.SCALAR, true);
        var desc2 = new NativeFunctionDescriptor(
                "fn", "INT", List.of("INT"),
                NativeFunctionDescriptor.FunctionType.SCALAR, true);

        assertThat(desc1).isEqualTo(desc2);
        assertThat(desc1.hashCode()).isEqualTo(desc2.hashCode());
    }
}

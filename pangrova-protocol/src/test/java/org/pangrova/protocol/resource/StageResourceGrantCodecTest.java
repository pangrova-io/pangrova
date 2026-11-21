package org.pangrova.protocol.resource;

import java.util.Base64;
import java.util.stream.Stream;

import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.UnknownFieldSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.pangrova.protocol.v2.StageResourceGrant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class StageResourceGrantCodecTest {
    @ParameterizedTest(name = "rejects unsigned overflow in {0}")
    @MethodSource("overflowingGrants")
    void rejectsUnsignedValuesOutsideJavaAccounting(String field, StageResourceGrant grant) {
        assertThatThrownBy(() -> StageResourceGrantCodec.encode(grant))
                .isInstanceOf(IllegalArgumentException.class);
        String encoded = Base64.getEncoder().encodeToString(grant.toByteArray());
        assertThatThrownBy(() -> StageResourceGrantCodec.decode(encoded))
                .isInstanceOf(IllegalArgumentException.class);
    }

    static Stream<Arguments> overflowingGrants() {
        return StageResourceGrant.getDescriptor().getFields().stream()
                .filter(field -> field.getJavaType() == FieldDescriptor.JavaType.LONG
                        || field.getJavaType() == FieldDescriptor.JavaType.INT)
                .map(field -> {
                    Object overflow = field.getJavaType() == FieldDescriptor.JavaType.INT
                            ? (Object) Integer.valueOf(-1) : Long.valueOf(-1);
                    return Arguments.of(field.getName(), valid().toBuilder().setField(field, overflow).build());
                });
    }

    @Test
    void rejectsUnknownGrantFieldsAtBothTransportDirections() {
        var fields = UnknownFieldSet.newBuilder().addField(999,
                UnknownFieldSet.Field.newBuilder().addVarint(1).build()).build();
        var grant = valid().toBuilder().setUnknownFields(fields).build();
        assertThatThrownBy(() -> StageResourceGrantCodec.encode(grant))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> StageResourceGrantCodec.decode(
                Base64.getEncoder().encodeToString(grant.toByteArray())))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void preservesValidGrantAndExplicitOptionalAbsence() {
        assertThat(StageResourceGrantCodec.decode(StageResourceGrantCodec.encode(valid()))).isEqualTo(valid());
        assertThat(StageResourceGrantCodec.decodeOptional(null)).isEqualTo(StageResourceGrant.getDefaultInstance());
        assertThatThrownBy(() -> StageResourceGrantCodec.decode(""))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void sourcePreparationGrantDoesNotClaimRelationalExecutionResources() {
        var preparation = valid().toBuilder().clearNativeMemoryBytes().setJvmHeapBytes(1024).build();
        org.assertj.core.api.Assertions.assertThatCode(() ->
                StageResourceGrants.validateSourcePreparation(preparation)).doesNotThrowAnyException();
        assertThatThrownBy(() -> StageResourceGrantCodec.encode(preparation))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> StageResourceGrants.validateSourcePreparation(valid()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("JVM memory");
    }

    private static StageResourceGrant valid() {
        return StageResourceGrant.newBuilder().setGrantId("grant").setCpuSlots(1)
                .setNativeMemoryBytes(1024).build();
    }
}

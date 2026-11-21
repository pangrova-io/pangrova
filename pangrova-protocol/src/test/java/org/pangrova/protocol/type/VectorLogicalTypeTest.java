package org.pangrova.protocol.type;

import org.junit.jupiter.api.Test;
import org.pangrova.protocol.v2.LogicalType;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class VectorLogicalTypeTest {

    @Test
    void storagePreservesDoubleAndBothNullabilityLevelsWithoutWeakeningAi() {
        var storage = VectorLogicalType.create(3, VectorLogicalType.FLOAT64, false, true);
        assertThat(VectorLogicalType.requireStorageDimension(storage)).isEqualTo(3);
        assertThat(storage.getCanonicalSql()).isEqualTo("VECTOR(3,DOUBLE)");
        assertThat(storage.getSubstraitType().getList().getNullability())
                .isEqualTo(io.substrait.proto.Type.Nullability.NULLABILITY_REQUIRED);
        assertThat(storage.getSubstraitType().getList().getType().getFp64().getNullability())
                .isEqualTo(io.substrait.proto.Type.Nullability.NULLABILITY_NULLABLE);
        assertThatThrownBy(() -> VectorLogicalType.requireDimension(storage))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> VectorLogicalType.requireDimension(
                VectorLogicalType.create(3, VectorLogicalType.FLOAT32, true, true)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void storageRejectsTamperedPrecisionDimensionAndNullability() {
        var storage = VectorLogicalType.create(9, VectorLogicalType.FLOAT64, true, true);
        assertThatThrownBy(() -> VectorLogicalType.requireStorageDimension(
                storage.toBuilder().setCanonicalSql("VECTOR(9)").build()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> VectorLogicalType.requireStorageDimension(storage.toBuilder()
                .setSubstraitType(storage.getSubstraitType().toBuilder().setList(
                        storage.getSubstraitType().getList().toBuilder()
                                .setNullability(io.substrait.proto.Type.Nullability.NULLABILITY_UNSPECIFIED))).build()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void roundTripsCanonicalDimensionAndPhysicalEncoding() {
        LogicalType type = VectorLogicalType.create(1_536);

        assertThat(VectorLogicalType.requireDimension(type)).isEqualTo(1_536);
        assertThat(type.getCanonicalSql()).isEqualTo("VECTOR(1536)");
        assertThat(type.getSubstraitType().hasList()).isTrue();
        assertThat(type.getSubstraitType().getList().getType().hasFp32()).isTrue();
    }

    @Test
    void rejectsUnboundedAndTamperedVectorContracts() {
        assertThatThrownBy(() -> VectorLogicalType.requireDimension(
                LogicalType.newBuilder().setCanonicalSql("VECTOR").build()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> VectorLogicalType.requireDimension(
                VectorLogicalType.create(384).toBuilder()
                        .setCanonicalSql("VECTOR(768)").build()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsDimensionsOutsideStorageContract() {
        assertThatThrownBy(() -> VectorLogicalType.create(0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> VectorLogicalType.create(65_536))
                .isInstanceOf(IllegalArgumentException.class);
    }
}

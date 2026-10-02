package org.pangrova.connector.paimon.type;

import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Test;
import org.pangrova.spi.connector.data.type.ArrayType;
import org.pangrova.spi.connector.data.type.PangrovaType;
import org.pangrova.spi.connector.data.type.VectorType;
import org.pangrova.spi.connector.data.type.primitive.BooleanType;
import org.pangrova.spi.connector.data.type.primitive.IntegerType;

import static org.assertj.core.api.Assertions.assertThat;

/** SDK fixed-size vectors retain their actual logical dimension and element precision. */
class PaimonSdkVectorTypeTest {
    @Test
    void sdkFloatAndDoubleVectorsAreLogicalVectorsNotUnconstrainedArrays() {
        var single = DataTypes.VECTOR(3, DataTypes.FLOAT());
        var doubled = DataTypes.VECTOR(9, DataTypes.DOUBLE());
        assertThat(PaimonTypeMapper.toPangrovaType(single)).isEqualTo(PangrovaType.VECTOR);
        assertThat(PaimonTypeMapper.toParameterizedType(single)).isEqualTo(new VectorType(3, null));
        assertThat(PaimonTypeMapper.toPangrovaType(doubled)).isEqualTo(PangrovaType.VECTOR);
        assertThat(PaimonTypeMapper.toParameterizedType(doubled))
                .isEqualTo(new VectorType(9, VectorType.ElementType.DOUBLE));
    }

    @Test
    void sdkNonFloatingVectorsKeepActualElementsRatherThanBeingCoercedToFloat() {
        var integer = DataTypes.VECTOR(4, DataTypes.INT());
        var bool = DataTypes.VECTOR(4, DataTypes.BOOLEAN());
        assertThat(PaimonTypeMapper.toPangrovaType(integer)).isEqualTo(PangrovaType.ARRAY);
        assertThat(PaimonTypeMapper.toParameterizedType(integer)).isEqualTo(new ArrayType(IntegerType.INSTANCE));
        assertThat(PaimonTypeMapper.toPangrovaType(bool)).isEqualTo(PangrovaType.ARRAY);
        assertThat(PaimonTypeMapper.toParameterizedType(bool)).isEqualTo(new ArrayType(BooleanType.INSTANCE));
    }
}

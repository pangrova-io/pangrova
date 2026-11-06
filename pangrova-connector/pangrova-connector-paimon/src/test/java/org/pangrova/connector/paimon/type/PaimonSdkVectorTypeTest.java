package org.pangrova.connector.paimon.type;

import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Test;
import org.pangrova.types.ArrayType;
import org.pangrova.types.PangrovaType;
import org.pangrova.types.VectorType;
import org.pangrova.types.primitive.BooleanType;
import org.pangrova.types.primitive.IntegerType;

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

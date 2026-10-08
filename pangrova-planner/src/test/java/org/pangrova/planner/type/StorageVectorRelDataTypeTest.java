package org.pangrova.planner.type;

import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.pangrova.protocol.type.VectorLogicalType;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class StorageVectorRelDataTypeTest {
    @Test
    void commonVectorBranchesPreserveDimensionAndPromoteCoordinatesWhileArrayBranchesEraseIdentity() {
        var factory = new PlannerTypeFactory();
        var fp32 = factory.createStorageVector(VectorLogicalType.create(3, VectorLogicalType.FLOAT32, false, false));
        var fp64 = factory.createStorageVector(VectorLogicalType.create(3, VectorLogicalType.FLOAT64, true, false));
        var common = factory.leastRestrictive(List.of(fp32, fp64, factory.createSqlType(SqlTypeName.NULL)));
        assertThat(common).isInstanceOf(StorageVectorRelDataType.class);
        assertThat(((StorageVectorRelDataType) common).logicalType())
                .isEqualTo(VectorLogicalType.create(3, VectorLogicalType.FLOAT64, true, false));
        var differentDimension = factory.createStorageVector(VectorLogicalType.create(4, VectorLogicalType.FLOAT32, false, false));
        assertThat(factory.leastRestrictive(List.of(fp32, differentDimension)))
                .isNotInstanceOf(StorageVectorRelDataType.class);
        var plain = factory.createArrayType(factory.createSqlType(SqlTypeName.REAL), -1);
        assertThat(factory.leastRestrictive(List.of(fp32, plain))).isNotInstanceOf(StorageVectorRelDataType.class);
    }

    @ParameterizedTest
    @CsvSource({"1,FLOAT32,false,false", "65535,FLOAT64,true,false", "2,FLOAT32,true,true"})
    void typeCopiesAndNullabilityChangesRetainTheCanonicalVectorIdentity(
            int dimension, String element, boolean nullable, boolean elementNullable) {
        var factory = new PlannerTypeFactory();
        var declaration = VectorLogicalType.create(dimension, element, nullable, elementNullable);
        var vector = factory.createStorageVector(declaration);
        assertThat(vector.logicalType()).isEqualTo(declaration);
        assertThat(factory.copyType(vector)).isInstanceOf(StorageVectorRelDataType.class).isEqualTo(vector);

        for (boolean resultNullable : List.of(true, false)) {
            var expected = VectorLogicalType.create(dimension, element, resultNullable, elementNullable);
            var copied = factory.createTypeWithNullability(vector, resultNullable);
            assertThat(copied).isInstanceOf(StorageVectorRelDataType.class);
            assertThat(((StorageVectorRelDataType) copied).logicalType()).isEqualTo(expected);
            assertThat(factory.enforceTypeWithNullability(vector, resultNullable)).isEqualTo(copied);
        }
    }

    @ParameterizedTest
    @CsvSource({"FLOAT32,REAL", "FLOAT64,DOUBLE"})
    void equalityAndDigestDistinguishDimensionAndPlainArrays(String element, SqlTypeName sqlElement) {
        var factory = new PlannerTypeFactory();
        var vector = factory.createStorageVector(VectorLogicalType.create(3, element, true, false));
        var different = factory.createStorageVector(VectorLogicalType.create(4, element, true, false));
        assertThat(vector).isNotEqualTo(different);
        assertThat(vector.deepEquals(different)).isFalse();
        assertThat(vector.getFullTypeString()).contains("VECTOR(3");
        assertThat(vector).isNotEqualTo(factory.createTypeWithNullability(
                factory.createArrayType(factory.createSqlType(sqlElement), -1), true));
    }
}

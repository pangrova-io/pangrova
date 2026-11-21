package org.pangrova.planner.type;

import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ModelBoundVectorRelDataTypeTest {

    @Test
    void remainsDistinctFromAnUnboundedSqlArray() {
        var factory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        var real = factory.createSqlType(SqlTypeName.REAL);

        var vector = new ModelBoundVectorRelDataType(real, true);

        assertThat(vector.getSqlTypeName()).isEqualTo(SqlTypeName.ARRAY);
        assertThat(vector.getComponentType()).isEqualTo(real);
        assertThat(vector.getFullTypeString()).contains("VECTOR(EXACT_MODEL)");
        assertThat(vector).isNotEqualTo(factory.createArrayType(real, -1));
    }
}

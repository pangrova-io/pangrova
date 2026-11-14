package org.pangrova.planner.metadata.catalog;

import java.util.List;
import java.util.OptionalInt;

import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;
import org.pangrova.types.enums.PangrovaType;
import org.pangrova.types.VectorType;
import org.pangrova.connector.api.metadata.table.ConnectorTableMetadata;

import static org.assertj.core.api.Assertions.assertThat;

class MetadataResolvedTableVectorTypeTest {

    @Test
    void preservesStoredVectorElementPrecisionInPlannerSchema() {
        var columns = List.of(
                new ConnectorTableMetadata.Column("float_vector", PangrovaType.VECTOR,
                        OptionalInt.empty(), new VectorType(3, VectorType.ElementType.FLOAT)),
                new ConnectorTableMetadata.Column("double_vector", PangrovaType.VECTOR,
                        OptionalInt.empty(), new VectorType(3, VectorType.ElementType.DOUBLE)));
        var metadata = new ConnectorTableMetadata(
                new org.pangrova.connector.api.metadata.table.SchemaTableName("schema", "vectors"),
                columns.stream().map(ConnectorTableMetadata.Column::name).toList(), columns, java.util.Map.of());
        var table = new MetadataResolvedTable(metadata);

        var row = table.getRowType(new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT));
        assertThat(row.getFieldList().get(0).getType().getComponentType().getSqlTypeName())
                .isEqualTo(SqlTypeName.REAL);
        assertThat(row.getFieldList().get(1).getType().getComponentType().getSqlTypeName())
                .isEqualTo(SqlTypeName.DOUBLE);
    }
}

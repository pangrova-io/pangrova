package org.pangrova.metadata.service;

import org.junit.jupiter.api.Test;
import org.pangrova.metadata.model.ColumnMetadata;
import org.pangrova.metadata.model.ColumnType;
import org.pangrova.spi.connector.data.type.PangrovaType;
import org.pangrova.spi.connector.data.type.primitive.DecimalType;
import org.pangrova.spi.connector.data.type.primitive.VarcharType;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MetadataColumnDeclarationsTest {
    @Test
    void keepsScalarTypesAndDeclaredParametersWithoutInventingFieldIds() {
        var decimal = column(ColumnType.decimal(18, 4));
        assertThat(decimal.type()).isEqualTo(PangrovaType.DECIMAL);
        assertThat(decimal.parameterizedType()).isEqualTo(new DecimalType(18, 4));
        assertThat(decimal.fieldId()).isEmpty();
        var varchar = column(ColumnType.varchar(255));
        assertThat(varchar.parameterizedType()).isEqualTo(new VarcharType(255));
        assertThat(column(ColumnType.of("string")).parameterizedType()).isEqualTo(VarcharType.UNBOUNDED);
        assertThat(column(ColumnType.of("int")).type()).isEqualTo(PangrovaType.INTEGER);
        assertThat(column(ColumnType.of("TIMESTAMP_NS")).type()).isEqualTo(PangrovaType.TIMESTAMP_NS);
    }

    @Test
    void rejectsMissingOrInvalidParametersInsteadOfSelectingDecimalDefaults() {
        for (var type : List.of(ColumnType.of("DECIMAL"), ColumnType.decimal(39, 0),
                ColumnType.decimal(2, 3), ColumnType.varchar(-1), new ColumnType("BIGINT", 10, 0, 0),
                new ColumnType("DECIMAL", 18, 2, 10), new ColumnType("BIGINT", 0, 0, 10))) {
            assertThatThrownBy(() -> column(type)).isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void rejectsIncompleteStructuredTypesInsteadOfInventingChildTypes() {
        for (String name : List.of("ARRAY", "MAP", "STRUCT", "VECTOR", "UNKNOWN", "VARCHAR_FAKE")) {
            assertThatThrownBy(() -> column(ColumnType.of(name))).isInstanceOf(IllegalArgumentException.class);
        }
    }

    private static org.pangrova.spi.connector.metadata.table.ConnectorTableMetadata.Column column(ColumnType type) {
        return MetadataColumnDeclarations.connectorColumn(new ColumnMetadata("value", type, true, "", Map.of()));
    }
}

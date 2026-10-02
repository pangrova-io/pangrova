package org.pangrova.connector.paimon.schema;

import org.pangrova.spi.connector.data.type.PangrovaType;
import org.pangrova.spi.connector.metadata.table.ConnectorTableMetadata;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DecimalType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.OptionalInt;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for {@link PaimonSchemaBuilder}, verifying that DECIMAL
 * columns preserve their declared precision/scale instead of being hardcoded
 * to DECIMAL(38,18) via the precision-less enum path.
 */
class PaimonSchemaBuilderTest {

    @Test
    void lanceVectorKeepsDimensionAndDoubleElementInActualSdkSchema() {
        var vector = new org.pangrova.spi.connector.data.type.VectorType(3,
                org.pangrova.spi.connector.data.type.VectorType.ElementType.DOUBLE);
        var schema = schema(new ConnectorTableMetadata.Column("embedding", PangrovaType.VECTOR,
                OptionalInt.empty(), vector), Map.of("file.format", "lance"));
        var actual = (org.apache.paimon.types.VectorType) schema.fields().get(0).type();
        assertThat(actual.getLength()).isEqualTo(3);
        assertThat(actual.getElementType()).isEqualTo(new org.apache.paimon.types.DoubleType());
        assertThat(actual.isNullable()).isTrue();
        assertThat(schema.options()).containsEntry("pangrova.vector.dimension.embedding", "3");
    }

    @Test
    void parquetVectorKeepsExistingArrayStorageWithDeclaredDimensionMetadata() {
        var vector = new org.pangrova.spi.connector.data.type.VectorType(9, null);
        var schema = schema(new ConnectorTableMetadata.Column("embedding", PangrovaType.VECTOR,
                OptionalInt.empty(), vector), Map.of());
        assertThat(schema.fields().get(0).type()).isEqualTo(
                new org.apache.paimon.types.ArrayType(new org.apache.paimon.types.FloatType()));
        assertThat(schema.options()).containsEntry("file.format", "parquet")
                .containsEntry("pangrova.vector.dimension.embedding", "9");
    }

    @Test
    void vectorPropertyCannotOverrideTheDeclaredDimension() {
        var vector = new org.pangrova.spi.connector.data.type.VectorType(3, null);
        var column = new ConnectorTableMetadata.Column("embedding", PangrovaType.VECTOR,
                OptionalInt.empty(), vector);
        assertThatThrownBy(() -> schema(column, Map.of("pangrova.vector.dimension.embedding", "4")))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("disagrees");
    }

    @Test
    void nestedArrayMapRowKeepsAllFieldsDecimalAndCharacterParameters() {
        var row = new org.pangrova.spi.connector.data.type.RowType(List.of(
                new org.pangrova.spi.connector.data.type.RowField("amount",
                        new org.pangrova.spi.connector.data.type.primitive.DecimalType(38, 17)),
                new org.pangrova.spi.connector.data.type.RowField("label",
                        new org.pangrova.spi.connector.data.type.primitive.VarcharType(27))));
        var nested = new org.pangrova.spi.connector.data.type.ArrayType(
                new org.pangrova.spi.connector.data.type.MapType(
                        new org.pangrova.spi.connector.data.type.primitive.CharType(7), row));
        var schema = schema(new ConnectorTableMetadata.Column("items", PangrovaType.ARRAY,
                OptionalInt.empty(), nested), Map.of());
        var array = (org.apache.paimon.types.ArrayType) schema.fields().get(0).type();
        var map = (org.apache.paimon.types.MapType) array.getElementType();
        assertThat(map.getKeyType()).isEqualTo(new org.apache.paimon.types.CharType(7));
        var actualRow = (org.apache.paimon.types.RowType) map.getValueType();
        assertThat(actualRow.getFieldNames()).containsExactly("amount", "label");
        assertThat(actualRow.getFields().get(0).type()).isEqualTo(new DecimalType(38, 17));
        assertThat(actualRow.getFields().get(1).type()).isEqualTo(new org.apache.paimon.types.VarCharType(27));
        assertThat(actualRow.getFields()).extracting(DataField::id).containsExactly(1, 2);
    }

    private static Schema schema(ConnectorTableMetadata.Column column, Map<String, String> properties) {
        return PaimonSchemaBuilder.buildPaimonSchema(new ConnectorTableMetadata(
                new SchemaTableName("db", "t"), List.of(column.name()), List.of(column), properties));
    }

    @Test
    void buildPaimonSchema_decimalColumn_preservesDeclaredPrecisionAndScale() {
        ConnectorTableMetadata.Column decimalCol = new ConnectorTableMetadata.Column(
                "amount", PangrovaType.DECIMAL, OptionalInt.empty(),
                new org.pangrova.spi.connector.data.type.primitive.DecimalType(12, 4));
        ConnectorTableMetadata metadata = new ConnectorTableMetadata(
                new SchemaTableName("db", "t"),
                List.of("amount"),
                List.of(decimalCol),
                Map.of());

        Schema schema = PaimonSchemaBuilder.buildPaimonSchema(metadata);

        DataField field = schema.fields().get(0);
        assertThat(field.type()).isInstanceOf(DecimalType.class);
        DecimalType dt = (DecimalType) field.type();
        assertThat(dt.getPrecision()).isEqualTo(12);
        assertThat(dt.getScale()).isEqualTo(4);
    }

    @Test
    void buildPaimonSchema_decimalColumnWithoutParameterizedType_fallsBackToEnum() {
        // No parameterizedType -> legacy enum path -> DECIMAL(38,18).
        ConnectorTableMetadata.Column decimalCol =
                new ConnectorTableMetadata.Column("amount", PangrovaType.DECIMAL);
        ConnectorTableMetadata metadata = new ConnectorTableMetadata(
                new SchemaTableName("db", "t"),
                List.of("amount"),
                List.of(decimalCol),
                Map.of());

        Schema schema = PaimonSchemaBuilder.buildPaimonSchema(metadata);

        DecimalType dt = (DecimalType) schema.fields().get(0).type();
        assertThat(dt.getPrecision()).isEqualTo(38);
        assertThat(dt.getScale()).isEqualTo(18);
    }
}

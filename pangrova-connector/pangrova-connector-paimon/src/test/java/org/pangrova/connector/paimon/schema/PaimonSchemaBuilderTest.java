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

/**
 * Regression tests for {@link PaimonSchemaBuilder}, verifying that DECIMAL
 * columns preserve their declared precision/scale instead of being hardcoded
 * to DECIMAL(38,18) via the precision-less enum path.
 */
class PaimonSchemaBuilderTest {

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

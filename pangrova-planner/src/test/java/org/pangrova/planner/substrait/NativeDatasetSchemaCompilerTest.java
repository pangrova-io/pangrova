package org.pangrova.planner.substrait;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.pangrova.protocol.type.VectorLogicalType;
import org.pangrova.connector.spi.data.type.PangrovaType;
import org.pangrova.connector.spi.data.type.RowField;
import org.pangrova.connector.spi.data.type.RowType;
import org.pangrova.connector.spi.data.type.VectorType;
import org.pangrova.connector.spi.data.type.primitive.BigIntType;
import org.pangrova.connector.spi.data.type.primitive.DecimalType;
import org.pangrova.connector.spi.metadata.table.ConnectorTableMetadata;

import java.util.List;
import java.util.OptionalInt;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class NativeDatasetSchemaCompilerTest {
    @ParameterizedTest
    @ValueSource(ints = {0, 18, 38})
    void nativeDatasetAndLogicalColumnPreserveDecimal38(int scale) {
        var result = NativeDatasetSchemaCompiler.compile(List.of(
                column("amount", PangrovaType.DECIMAL, new DecimalType(38, scale))));
        var physical = result.schema().getStruct().getTypes(0);
        assertThat(physical.getDecimal().getPrecision()).isEqualTo(38);
        assertThat(physical.getDecimal().getScale()).isEqualTo(scale);
        assertThat(physical.getDecimal().getNullability())
                .isEqualTo(io.substrait.proto.Type.Nullability.NULLABILITY_NULLABLE);
        assertThat(result.columnTypes().get(0).getSubstraitType()).isEqualTo(physical);
        assertThat(result.columnTypes().get(0).getCanonicalSql()).isEqualTo(new DecimalType(38, scale).getDisplayName());
    }
    @Test void nestedNamesAndParameterizedStorageUseThePlannerSchema() {
        var columns = List.of(column("payload", PangrovaType.STRUCT,
                new RowType(List.of(new RowField("id", BigIntType.INSTANCE)))),
                column("embedding", PangrovaType.VECTOR, new VectorType(3, VectorType.ElementType.FLOAT)),
                column("amount", PangrovaType.DECIMAL, new DecimalType(18, 4)),
                new ConnectorTableMetadata.Column("created_at", PangrovaType.TIMESTAMP_NS));
        var result = NativeDatasetSchemaCompiler.compile(columns);
        assertThat(result.schema().getNamesList()).containsExactly("payload", "id", "embedding", "amount", "created_at");
        assertThat(result.schema().getStruct().getTypesCount()).isEqualTo(4);
        assertThat(result.columnTypes()).hasSize(4);
        assertThat(result.columnTypes().get(1)).isEqualTo(VectorLogicalType.create(3));
        assertThat(result.schema().getStruct().getTypes(2).getDecimal().getScale()).isEqualTo(4);
        assertThat(result.schema().getStruct().getTypes(3).getPrecisionTimestamp().getPrecision()).isEqualTo(9);
    }
    @Test void doubleVectorNeverNarrowsToFloat32() {
        var result = NativeDatasetSchemaCompiler.compile(List.of(column("embedding", PangrovaType.VECTOR,
                new VectorType(2, VectorType.ElementType.DOUBLE))));
        assertThat(result.schema().getStruct().getTypes(0).getList().getType().hasFp64()).isTrue();
        assertThat(result.columnTypes().get(0).getConstraints(1).getStringValue()).isEqualTo("FLOAT64");
    }
    @Test void incompleteOrDuplicateSchemasFailBeforeAnyWorkerRpc() {
        assertThatThrownBy(() -> NativeDatasetSchemaCompiler.compile(List.of())).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> NativeDatasetSchemaCompiler.compile(List.of(
                new ConnectorTableMetadata.Column("x", PangrovaType.ARRAY)))).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> NativeDatasetSchemaCompiler.compile(List.of(
                new ConnectorTableMetadata.Column("x", PangrovaType.UNKNOWN)))).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> NativeDatasetSchemaCompiler.compile(List.of(
                new ConnectorTableMetadata.Column("x", PangrovaType.BIGINT),
                new ConnectorTableMetadata.Column("x", PangrovaType.BIGINT)))).isInstanceOf(IllegalArgumentException.class);
    }
    private static ConnectorTableMetadata.Column column(String name, PangrovaType type,
            org.pangrova.connector.spi.data.type.Type parameters) {
        return new ConnectorTableMetadata.Column(name, type, OptionalInt.empty(), parameters);
    }
}

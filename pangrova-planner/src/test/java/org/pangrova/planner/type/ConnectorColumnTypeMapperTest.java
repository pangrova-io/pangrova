package org.pangrova.planner.type;

import org.pangrova.types.enums.VectorElementType;

import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.pangrova.planner.substrait.NativeDatasetSchemaCompiler;
import org.pangrova.protocol.type.VectorLogicalType;
import org.pangrova.types.ArrayType;
import org.pangrova.types.MapType;
import org.pangrova.types.NullOnlyType;
import org.pangrova.types.enums.PangrovaType;
import org.pangrova.types.mapping.PangrovaTypeConverter;
import org.pangrova.types.TypeSignature;
import org.pangrova.types.RowField;
import org.pangrova.types.RowType;
import org.pangrova.types.Type;
import org.pangrova.types.VectorType;
import org.pangrova.types.primitive.BigIntType;
import org.pangrova.types.primitive.DecimalType;
import org.pangrova.types.primitive.VarcharType;
import org.pangrova.connector.api.metadata.table.ConnectorTableMetadata;

import java.util.List;
import java.util.OptionalInt;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Exercises the same metadata adapter used by SQL validation and Native schema compilation. */
class ConnectorColumnTypeMapperTest {
    @Test
    void uuidDeclarationRetainsIdentityThroughScalarAndNestedStorageSchemas() {
        Type uuid = PangrovaTypeConverter.toType(PangrovaType.UUID);
        assertThat(uuid.getTypeSignature()).isEqualTo(new TypeSignature("UUID"));
        assertThat(PangrovaTypeConverter.toPangrovaType(uuid)).isEqualTo(PangrovaType.UUID);
        assertThat(new StandardTypeRegistry().getType(uuid.getTypeSignature())).isEqualTo(uuid);
        var columns = List.of(column(PangrovaType.UUID, uuid),
                new ConnectorTableMetadata.Column("nested", PangrovaType.STRUCT, OptionalInt.empty(),
                        new RowType(List.of(new RowField("id", uuid),
                                new RowField("ids", new ArrayType(uuid)),
                                new RowField("lookup", new MapType(uuid, uuid))))));
        var sql = ConnectorColumnTypeMapper.rowType(columns, new PlannerTypeFactory());
        assertThat(sql.getFieldList().get(0).getType().getSqlTypeName()).isEqualTo(SqlTypeName.UUID);
        var schema = NativeDatasetSchemaCompiler.compile(columns).schema();
        assertThat(schema.getNamesList()).containsExactly("value", "nested", "id", "ids", "lookup");
        assertThat(schema.getStruct().getTypes(0).hasUuid()).isTrue();
        var nested = schema.getStruct().getTypes(1).getStruct();
        assertThat(nested.getTypes(0).hasUuid()).isTrue();
        assertThat(nested.getTypes(1).getList().getType().hasUuid()).isTrue();
        assertThat(nested.getTypes(2).getMap().getKey().hasUuid()).isTrue();
        assertThat(nested.getTypes(2).getMap().getValue().hasUuid()).isTrue();
    }

    @ParameterizedTest
    @EnumSource(value = PangrovaType.class, names = {"UNKNOWN", "ARRAY", "VECTOR", "MAP", "STRUCT", "DECIMAL"})
    void incompleteMetadataNeverInventsAStorageType(PangrovaType type) {
        assertThatThrownBy(() -> ConnectorColumnTypeMapper.rowType(
                List.of(new ConnectorTableMetadata.Column("value", type)), new PlannerTypeFactory()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining(type.name());
    }

    @ParameterizedTest
    @EnumSource(value = PangrovaType.class, names = {"ARRAY", "VECTOR", "MAP", "STRUCT", "DECIMAL"})
    void mismatchedMetadataNeverAcquiresTheDeclaredTypeIdentity(PangrovaType type) {
        assertThatThrownBy(() -> ConnectorColumnTypeMapper.rowType(
                List.of(column(type, BigIntType.INSTANCE)), new PlannerTypeFactory()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining(type.name());
    }

    @Test
    void explicitNullOnlyDeclarationIsNullableButBareUnknownRemainsRejected() {
        var sql = ConnectorColumnTypeMapper.rowType(
                List.of(column(PangrovaType.UNKNOWN, NullOnlyType.INSTANCE)),
                new PlannerTypeFactory()).getFieldList().get(0).getType();
        assertThat(sql.getSqlTypeName()).isEqualTo(SqlTypeName.VARCHAR);
        assertThat(sql.isNullable()).isTrue();
        var compiled = NativeDatasetSchemaCompiler.compile(
                List.of(column(PangrovaType.UNKNOWN, NullOnlyType.INSTANCE)));
        assertThat(compiled.schema().getStruct().getTypes(0).hasString()).isTrue();
        assertThatThrownBy(() -> ConnectorColumnTypeMapper.rowType(
                List.of(column(PangrovaType.UNKNOWN, VarcharType.UNBOUNDED)),
                new PlannerTypeFactory()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("UNKNOWN");
    }

    @Test
    void completeNestedTypesPreserveParametersAndOrderedNames() {
        var columns = List.of(column(PangrovaType.ARRAY, new ArrayType(new DecimalType(28, 9))),
                new ConnectorTableMetadata.Column("map", PangrovaType.MAP, OptionalInt.empty(),
                        new MapType(VarcharType.UNBOUNDED, new RowType(List.of(
                                new RowField("amount", new DecimalType(38, 14)),
                                new RowField("id", BigIntType.INSTANCE))))));
        var compiled = NativeDatasetSchemaCompiler.compile(columns);
        assertThat(compiled.schema().getNamesList()).containsExactly("value", "map", "amount", "id");
        assertThat(compiled.schema().getStruct().getTypes(0).getList().getType().getDecimal().getScale())
                .isEqualTo(9);
        var row = compiled.schema().getStruct().getTypes(1).getMap().getValue().getStruct();
        assertThat(row.getTypes(0).getDecimal().getPrecision()).isEqualTo(38);
        assertThat(row.getTypes(0).getDecimal().getScale()).isEqualTo(14);
        assertThat(row.getTypes(1).hasI64()).isTrue();
    }

    @ParameterizedTest
    @EnumSource(VectorElementType.class)
    void vectorSchemaAndLogicalContractHaveTheSameElementPrecisionAndNullability(VectorElementType element) {
        var columns = List.of(column(PangrovaType.VECTOR, new VectorType(65535, element)));
        var sql = ConnectorColumnTypeMapper.rowType(columns, new PlannerTypeFactory()).getFieldList().get(0).getType();
        assertThat(sql.getComponentType().getSqlTypeName()).isEqualTo(
                element == VectorElementType.FLOAT ? SqlTypeName.REAL : SqlTypeName.DOUBLE);
        var compiled = NativeDatasetSchemaCompiler.compile(columns);
        var logical = compiled.columnTypes().get(0);
        assertThat(logical).isEqualTo(VectorLogicalType.create(65535,
                element == VectorElementType.FLOAT ? VectorLogicalType.FLOAT32 : VectorLogicalType.FLOAT64,
                true, false));
        assertThat(logical.getSubstraitType()).isEqualTo(compiled.schema().getStruct().getTypes(0));
        assertThat(VectorLogicalType.requireStorageDimension(logical)).isEqualTo(65535);
    }

    private static ConnectorTableMetadata.Column column(PangrovaType type, Type parameters) {
        return new ConnectorTableMetadata.Column("value", type, OptionalInt.empty(), parameters);
    }
}

package org.pangrova.planner.core.substrait;

import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.CsvSource;
import org.pangrova.planner.core.type.PlannerTypeSystem;

import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.avatica.util.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TypeMapperTest {

    @ParameterizedTest
    @CsvSource({"0,0", "-2147483648,2", "39,2", "10,-2", "10,11"})
    void malformedDecimalTypesFailBothCanonicalMappings(int precision, int scale) {
        var type = new BasicSqlType(PlannerTypeSystem.INSTANCE, SqlTypeName.DECIMAL, precision, scale);
        assertThatThrownBy(() -> typeMapper.map(type)).isInstanceOf(SubstraitConversionException.class)
                .hasMessageContaining("Invalid DECIMAL");
        assertThatThrownBy(() -> typeMapper.toPangrovaParameterizedType(type))
                .isInstanceOf(SubstraitConversionException.class).hasMessageContaining("Invalid DECIMAL");
    }

    @Test
    void omittedScaleSentinelResolvesToTheDeclaredDefaultRatherThanAMalformedType() {
        var type = new BasicSqlType(PlannerTypeSystem.INSTANCE, SqlTypeName.DECIMAL, 10,
                RelDataType.SCALE_NOT_SPECIFIED);
        assertThat(type.getScale()).isZero();
        assertThat(typeMapper.map(type)).isEqualTo(TypeCreator.REQUIRED.decimal(10, 0));
        assertThat(typeMapper.toPangrovaParameterizedType(type))
                .isEqualTo(new org.pangrova.types.primitive.DecimalType(10, 0));
    }

    private TypeMapper typeMapper;
    private RelDataTypeFactory typeFactory;

    @BeforeEach
    void setUp() {
        typeMapper = new TypeMapper();
        typeFactory = new SqlTypeFactoryImpl(org.apache.calcite.rel.type.RelDataTypeSystem.DEFAULT);
    }

    @Test
    void mapInteger() {
        RelDataType calciteType = typeFactory.createSqlType(SqlTypeName.INTEGER);
        Type result = typeMapper.map(calciteType);
        assertThat(result).isEqualTo(TypeCreator.REQUIRED.I32);
    }

    @Test
    void mapBigint() {
        RelDataType calciteType = typeFactory.createSqlType(SqlTypeName.BIGINT);
        Type result = typeMapper.map(calciteType);
        assertThat(result).isEqualTo(TypeCreator.REQUIRED.I64);
    }

    @Test
    void mapVarchar() {
        RelDataType calciteType = typeFactory.createSqlType(SqlTypeName.VARCHAR);
        Type result = typeMapper.map(calciteType);
        assertThat(result).isEqualTo(TypeCreator.REQUIRED.STRING);
    }

    @Test
    void mapBoolean() {
        RelDataType calciteType = typeFactory.createSqlType(SqlTypeName.BOOLEAN);
        Type result = typeMapper.map(calciteType);
        assertThat(result).isEqualTo(TypeCreator.REQUIRED.BOOLEAN);
    }

    @Test
    void mapDouble() {
        RelDataType calciteType = typeFactory.createSqlType(SqlTypeName.DOUBLE);
        Type result = typeMapper.map(calciteType);
        assertThat(result).isEqualTo(TypeCreator.REQUIRED.FP64);
    }

    @Test
    void mapDecimal() {
        RelDataType calciteType = typeFactory.createSqlType(SqlTypeName.DECIMAL, 10, 2);
        Type result = typeMapper.map(calciteType);
        assertThat(result).isEqualTo(TypeCreator.REQUIRED.decimal(10, 2));
    }

    @Test
    void mapDate() {
        RelDataType calciteType = typeFactory.createSqlType(SqlTypeName.DATE);
        Type result = typeMapper.map(calciteType);
        assertThat(result).isEqualTo(TypeCreator.REQUIRED.DATE);
    }

    @Test
    void mapTimestamp() {
        RelDataType calciteType = typeFactory.createSqlType(SqlTypeName.TIMESTAMP);
        Type result = typeMapper.map(calciteType);
        assertThat(result).isEqualTo(TypeCreator.REQUIRED.precisionTimestamp(
                calciteType.getPrecision()));
    }

    @Test
    void mapIntervalYearMonth() {
        SqlIntervalQualifier qualifier = new SqlIntervalQualifier(
                TimeUnit.YEAR, TimeUnit.MONTH, SqlParserPos.ZERO);
        RelDataType calciteType = typeFactory.createSqlIntervalType(qualifier);
        Type result = typeMapper.map(calciteType);
        assertThat(result).isEqualTo(TypeCreator.REQUIRED.INTERVAL_YEAR);
    }

    @Test
    void mapIntervalDaySecond() {
        SqlIntervalQualifier qualifier = new SqlIntervalQualifier(
                TimeUnit.DAY, TimeUnit.SECOND, SqlParserPos.ZERO);
        RelDataType calciteType = typeFactory.createSqlIntervalType(qualifier);
        Type result = typeMapper.map(calciteType);
        assertThat(result).isEqualTo(TypeCreator.REQUIRED.intervalDay(6));
    }

    @Test
    void mapNullableType() {
        RelDataType calciteType = typeFactory.createTypeWithNullability(
                typeFactory.createSqlType(SqlTypeName.INTEGER), true);
        Type result = typeMapper.map(calciteType);
        assertThat(result).isEqualTo(TypeCreator.NULLABLE.I32);
    }

    @Test
    void mapUnsupportedTypeThrows() {
        RelDataType calciteType = typeFactory.createSqlType(SqlTypeName.GEOMETRY);
        assertThatThrownBy(() -> typeMapper.map(calciteType))
                .isInstanceOf(SubstraitConversionException.class)
                .hasFieldOrPropertyWithValue("errorCode", PangrovaErrorCode.SUBSTRAIT_TYPE_MAPPING_FAILURE);
    }

    @Test
    void mapTinyint() {
        RelDataType calciteType = typeFactory.createSqlType(SqlTypeName.TINYINT);
        Type result = typeMapper.map(calciteType);
        assertThat(result).isEqualTo(TypeCreator.REQUIRED.I8);
    }

    @Test
    void mapSmallint() {
        RelDataType calciteType = typeFactory.createSqlType(SqlTypeName.SMALLINT);
        Type result = typeMapper.map(calciteType);
        assertThat(result).isEqualTo(TypeCreator.REQUIRED.I16);
    }

    @Test
    void mapFloat() {
        RelDataType calciteType = typeFactory.createSqlType(SqlTypeName.FLOAT);
        Type result = typeMapper.map(calciteType);
        assertThat(result).isEqualTo(TypeCreator.REQUIRED.FP32);
    }

    @Test
    void mapTime() {
        RelDataType calciteType = typeFactory.createSqlType(SqlTypeName.TIME);
        Type result = typeMapper.map(calciteType);
        assertThat(result).isEqualTo(TypeCreator.REQUIRED.precisionTime(
                calciteType.getPrecision()));
    }

    @Test
    void mapTimestampWithLocalTimeZone() {
        RelDataType calciteType = typeFactory.createSqlType(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE);
        Type result = typeMapper.map(calciteType);
        assertThat(result).isEqualTo(TypeCreator.REQUIRED.precisionTimestampTZ(
                calciteType.getPrecision()));
    }

    @Test
    void mapBinary() {
        RelDataType calciteType = typeFactory.createSqlType(SqlTypeName.BINARY);
        Type result = typeMapper.map(calciteType);
        assertThat(result).isEqualTo(TypeCreator.REQUIRED.BINARY);
    }

    @Test
    void mapVarbinary() {
        RelDataType calciteType = typeFactory.createSqlType(SqlTypeName.VARBINARY);
        Type result = typeMapper.map(calciteType);
        assertThat(result).isEqualTo(TypeCreator.REQUIRED.BINARY);
    }
}

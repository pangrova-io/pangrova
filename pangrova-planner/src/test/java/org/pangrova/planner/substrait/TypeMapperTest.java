package org.pangrova.planner.substrait;

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

import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.avatica.util.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TypeMapperTest {

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
        assertThat(result).isEqualTo(TypeCreator.REQUIRED.TIMESTAMP);
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
        assertThat(result).isEqualTo(TypeCreator.REQUIRED.INTERVAL_DAY);
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
        assertThat(result).isEqualTo(TypeCreator.REQUIRED.TIME);
    }

    @Test
    void mapTimestampWithLocalTimeZone() {
        RelDataType calciteType = typeFactory.createSqlType(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE);
        Type result = typeMapper.map(calciteType);
        assertThat(result).isEqualTo(TypeCreator.REQUIRED.TIMESTAMP_TZ);
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

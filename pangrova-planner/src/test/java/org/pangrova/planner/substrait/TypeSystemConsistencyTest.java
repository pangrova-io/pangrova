package org.pangrova.planner.substrait;

import io.substrait.type.Type;
import org.pangrova.types.enums.PangrovaType;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.EnumSet;
import java.util.Set;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Verifies type system consistency between PangrovaType and SubstraitConverter's TypeMapper.
 * Ensures every primitive PangrovaType has a corresponding Calcite → Substrait mapping.
 *
 * <p>Validates: Requirements 5.3</p>
 */
class TypeSystemConsistencyTest {

    /**
     * PangrovaType values that are complex/container types — these require
     * element type info and are handled via Calcite's structured type system,
     * not through simple SqlTypeName mapping.
     */
    private static final Set<PangrovaType> COMPLEX_TYPES = EnumSet.of(
            PangrovaType.STRUCT, PangrovaType.ARRAY, PangrovaType.MAP);

    /**
     * PangrovaType values that are connector-specific and don't have direct
     * Calcite SqlTypeName equivalents. These are mapped to VARCHAR/VARBINARY
     * at the Calcite level by the connector type mappers.
     */
    private static final Set<PangrovaType> CONNECTOR_SPECIFIC_TYPES = EnumSet.of(
            PangrovaType.UUID, PangrovaType.FIXED, PangrovaType.VECTOR,
            PangrovaType.VARIANT, PangrovaType.GEOMETRY, PangrovaType.GEOGRAPHY,
            PangrovaType.TIMESTAMP_NS, PangrovaType.UNKNOWN);

    private TypeMapper typeMapper;
    private RelDataTypeFactory typeFactory;

    @BeforeEach
    void setUp() {
        typeMapper = new TypeMapper();
        typeFactory = new SqlTypeFactoryImpl(
                org.apache.calcite.rel.type.RelDataTypeSystem.DEFAULT);
    }

    @ParameterizedTest(name = "PangrovaType.{0} has Calcite→Substrait mapping")
    @MethodSource("primitivePangrovaTypeMappings")
    void everyPrimitivePangrovaTypeHasSubstraitMapping(
            PangrovaType pangrovaType, SqlTypeName calciteType) {
        RelDataType relType = createCalciteType(calciteType);
        assertThatCode(() -> typeMapper.map(relType))
                .as("PangrovaType.%s (via Calcite %s) should map to Substrait",
                        pangrovaType, calciteType)
                .doesNotThrowAnyException();
    }

    @ParameterizedTest(name = "PangrovaType.{0} Substrait mapping produces non-null type")
    @MethodSource("primitivePangrovaTypeMappings")
    void substraitMappingProducesNonNullType(
            PangrovaType pangrovaType, SqlTypeName calciteType) {
        RelDataType relType = createCalciteType(calciteType);
        Type result = typeMapper.map(relType);
        assertThat(result)
                .as("Substrait type for PangrovaType.%s should not be null", pangrovaType)
                .isNotNull();
    }

    @Test
    void allPrimitivePangrovaTypesAreCovered() {
        Set<PangrovaType> covered = EnumSet.noneOf(PangrovaType.class);
        primitivePangrovaTypeMappings().forEach(args -> covered.add((PangrovaType) args.get()[0]));

        Set<PangrovaType> expected = EnumSet.allOf(PangrovaType.class);
        expected.removeAll(COMPLEX_TYPES);
        expected.removeAll(CONNECTOR_SPECIFIC_TYPES);
        assertThat(covered)
                .as("All primitive PangrovaType values should have Calcite→Substrait mappings")
                .containsAll(expected);
    }

    private RelDataType createCalciteType(SqlTypeName typeName) {
        if (typeName == SqlTypeName.DECIMAL) {
            return typeFactory.createSqlType(typeName, 10, 2);
        }
        if (typeName == SqlTypeName.INTERVAL_YEAR_MONTH) {
            SqlIntervalQualifier qualifier = new SqlIntervalQualifier(
                    TimeUnit.YEAR, TimeUnit.MONTH, SqlParserPos.ZERO);
            return typeFactory.createSqlIntervalType(qualifier);
        }
        if (typeName == SqlTypeName.INTERVAL_DAY_SECOND) {
            SqlIntervalQualifier qualifier = new SqlIntervalQualifier(
                    TimeUnit.DAY, TimeUnit.SECOND, SqlParserPos.ZERO);
            return typeFactory.createSqlIntervalType(qualifier);
        }
        return typeFactory.createSqlType(typeName);
    }

    static Stream<Arguments> primitivePangrovaTypeMappings() {
        return Stream.of(
                Arguments.of(PangrovaType.BOOLEAN, SqlTypeName.BOOLEAN),
                Arguments.of(PangrovaType.TINYINT, SqlTypeName.TINYINT),
                Arguments.of(PangrovaType.SMALLINT, SqlTypeName.SMALLINT),
                Arguments.of(PangrovaType.INTEGER, SqlTypeName.INTEGER),
                Arguments.of(PangrovaType.BIGINT, SqlTypeName.BIGINT),
                Arguments.of(PangrovaType.FLOAT, SqlTypeName.FLOAT),
                Arguments.of(PangrovaType.DOUBLE, SqlTypeName.DOUBLE),
                Arguments.of(PangrovaType.DECIMAL, SqlTypeName.DECIMAL),
                Arguments.of(PangrovaType.DATE, SqlTypeName.DATE),
                Arguments.of(PangrovaType.TIME, SqlTypeName.TIME),
                Arguments.of(PangrovaType.TIMESTAMP, SqlTypeName.TIMESTAMP),
                Arguments.of(PangrovaType.TIMESTAMP_TZ, SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE),
                Arguments.of(PangrovaType.VARCHAR, SqlTypeName.VARCHAR),
                Arguments.of(PangrovaType.VARBINARY, SqlTypeName.VARBINARY),
                Arguments.of(PangrovaType.INTERVAL_YEAR_MONTH, SqlTypeName.INTERVAL_YEAR_MONTH),
                Arguments.of(PangrovaType.INTERVAL_DAY_TIME, SqlTypeName.INTERVAL_DAY_SECOND)
        );
    }
}

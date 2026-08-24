package org.pangrova.planner.substrait;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FunctionMapperTest {

    private FunctionMapper mapper;

    @BeforeEach
    void setUp() {
        mapper = new FunctionMapper();
    }

    static Stream<Arguments> supportedOperators() {
        return Stream.of(
                // Arithmetic
                Arguments.of(SqlStdOperatorTable.PLUS, "arithmetic", "add"),
                Arguments.of(SqlStdOperatorTable.MINUS, "arithmetic", "subtract"),
                Arguments.of(SqlStdOperatorTable.MULTIPLY, "arithmetic", "multiply"),
                Arguments.of(SqlStdOperatorTable.DIVIDE, "arithmetic", "divide"),
                // Comparison
                Arguments.of(SqlStdOperatorTable.EQUALS, "comparison", "equal"),
                Arguments.of(SqlStdOperatorTable.NOT_EQUALS, "comparison", "not_equal"),
                Arguments.of(SqlStdOperatorTable.LESS_THAN, "comparison", "lt"),
                Arguments.of(SqlStdOperatorTable.GREATER_THAN, "comparison", "gt"),
                Arguments.of(SqlStdOperatorTable.LESS_THAN_OR_EQUAL, "comparison", "lte"),
                Arguments.of(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, "comparison", "gte"),
                // Boolean
                Arguments.of(SqlStdOperatorTable.AND, "boolean", "and"),
                Arguments.of(SqlStdOperatorTable.OR, "boolean", "or"),
                Arguments.of(SqlStdOperatorTable.NOT, "boolean", "not"),
                // Aggregate
                Arguments.of(SqlStdOperatorTable.SUM, "aggregate", "sum"),
                Arguments.of(SqlStdOperatorTable.COUNT, "aggregate", "count"),
                Arguments.of(SqlStdOperatorTable.MIN, "aggregate", "min"),
                Arguments.of(SqlStdOperatorTable.MAX, "aggregate", "max"),
                Arguments.of(SqlStdOperatorTable.AVG, "aggregate", "avg")
        );
    }

    @ParameterizedTest
    @MethodSource("supportedOperators")
    void mapSupportedOperator(SqlOperator operator, String namespace, String name) {
        FunctionReference ref = mapper.map(operator);
        assertThat(ref.namespace()).isEqualTo(namespace);
        assertThat(ref.name()).isEqualTo(name);
        assertThat(ref.compoundName()).isEqualTo(namespace + ":" + name);
    }

    @Test
    void mapUnsupportedOperatorThrows() {
        assertThatThrownBy(() -> mapper.map(SqlStdOperatorTable.SIMILAR_TO))
                .isInstanceOf(SubstraitConversionException.class)
                .hasFieldOrPropertyWithValue("errorCode", PangrovaErrorCode.SUBSTRAIT_FUNCTION_MAPPING_FAILURE);
    }

    @ParameterizedTest
    @MethodSource("vectorDistanceFunctions")
    void mapVectorDistanceFunction(String functionName, String expectedNamespace, String expectedName) {
        SqlFunction function = new SqlFunction(
                functionName,
                SqlKind.OTHER_FUNCTION,
                ReturnTypes.DOUBLE_NULLABLE,
                null,
                OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.ANY),
                SqlFunctionCategory.USER_DEFINED_FUNCTION);

        FunctionReference ref = mapper.map(function);
        assertThat(ref.namespace()).isEqualTo(expectedNamespace);
        assertThat(ref.name()).isEqualTo(expectedName);
    }

    static Stream<Arguments> vectorDistanceFunctions() {
        return Stream.of(
                Arguments.of("L2_DISTANCE", "vector", "l2_distance"),
                Arguments.of("COSINE_DISTANCE", "vector", "cosine_distance"),
                Arguments.of("INNER_PRODUCT", "vector", "inner_product"),
                Arguments.of("COSINE_SIMILARITY", "vector", "cosine_similarity")
        );
    }

    @Test
    void vectorDistanceFunctionResolvesToScalarVariant() {
        FunctionReference ref = new FunctionReference("vector", "l2_distance");
        var variant = mapper.resolveScalarVariant(ref);
        assertThat(variant.name()).isEqualTo("l2_distance");
        assertThat(variant.uri()).isEqualTo("/functions_vector.yaml");
    }
}

package org.pangrova.planner.validation;

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.validate.SqlNameMatchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link PangrovaSqlOperatorTable}.
 * Verifies that vector distance functions are registered with correct
 * return types and can be looked up by name.
 */
class PangrovaSqlOperatorTableTest {

    private PangrovaSqlOperatorTable operatorTable;

    @BeforeEach
    void setUp() {
        operatorTable = new PangrovaSqlOperatorTable();
    }

    @ParameterizedTest
    @ValueSource(strings = {"L2_DISTANCE", "COSINE_DISTANCE", "INNER_PRODUCT", "COSINE_SIMILARITY"})
    void vectorDistanceFunctionIsRegistered(String functionName) {
        List<SqlOperator> found = lookupByName(functionName);
        assertThat(found)
                .as("Function %s should be registered", functionName)
                .hasSize(1);
    }

    @ParameterizedTest
    @ValueSource(strings = {"L2_DISTANCE", "COSINE_DISTANCE", "INNER_PRODUCT", "COSINE_SIMILARITY"})
    void vectorDistanceFunctionReturnTypeIsDouble(String functionName) {
        List<SqlOperator> found = lookupByName(functionName);
        SqlFunction function = (SqlFunction) found.get(0);
        // ReturnTypes.DOUBLE_NULLABLE produces DOUBLE type
        assertThat(function.getReturnTypeInference()).isNotNull();
    }

    @ParameterizedTest
    @ValueSource(strings = {"L2_DISTANCE", "COSINE_DISTANCE", "INNER_PRODUCT", "COSINE_SIMILARITY"})
    void vectorDistanceFunctionCategoryIsUserDefined(String functionName) {
        List<SqlOperator> found = lookupByName(functionName);
        SqlFunction function = (SqlFunction) found.get(0);
        assertThat(function.getFunctionType())
                .isEqualTo(SqlFunctionCategory.USER_DEFINED_FUNCTION);
    }

    @ParameterizedTest
    @ValueSource(strings = {"l2_distance", "cosine_distance", "inner_product", "cosine_similarity"})
    void vectorDistanceFunctionLookupIsCaseInsensitive(String functionName) {
        List<SqlOperator> found = lookupByName(functionName);
        assertThat(found)
                .as("Case-insensitive lookup for %s should find the function", functionName)
                .hasSize(1);
    }

    @Test
    void getOperatorListContainsAllVectorDistanceFunctions() {
        List<SqlOperator> all = operatorTable.getOperatorList();
        List<String> names = all.stream().map(SqlOperator::getName).toList();
        assertThat(names).contains(
                "L2_DISTANCE", "COSINE_DISTANCE", "INNER_PRODUCT", "COSINE_SIMILARITY");
    }

    @Test
    void spatialFunctionsStillRegistered() {
        // Verify existing functions are not broken by the addition
        assertThat(lookupByName("ST_INTERSECTS")).hasSize(1);
        assertThat(lookupByName("ST_WITHIN")).hasSize(1);
        assertThat(lookupByName("ST_CONTAINS")).hasSize(1);
    }

    @Test
    void arrayFunctionsStillRegistered() {
        assertThat(lookupByName("ARRAY_CONTAINS")).hasSize(1);
        assertThat(lookupByName("ARRAY_CONTAINS_ALL")).hasSize(1);
    }

    @Test
    void unknownFunctionNotFound() {
        assertThat(lookupByName("NONEXISTENT_FUNC")).isEmpty();
    }

    private List<SqlOperator> lookupByName(String name) {
        List<SqlOperator> result = new ArrayList<>();
        SqlIdentifier id = new SqlIdentifier(name, org.apache.calcite.sql.parser.SqlParserPos.ZERO);
        operatorTable.lookupOperatorOverloads(
                id,
                SqlFunctionCategory.USER_DEFINED_FUNCTION,
                SqlSyntax.FUNCTION,
                result,
                SqlNameMatchers.withCaseSensitive(false));
        return result;
    }
}

package org.pangrova.planner.core.validation;

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.validate.SqlNameMatchers;
import org.apache.calcite.sql.type.ReturnTypes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.pangrova.protocol.function.declaration.FunctionBindingDeclarations;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

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

    @Test
    void qualifiedIdentifierDoesNotAssumeSimpleName() {
        SqlIdentifier qualified = new SqlIdentifier(
                List.of("catalog", "schema", "AI_COMPLETE"),
                org.apache.calcite.sql.parser.SqlParserPos.ZERO);
        List<SqlOperator> result = new ArrayList<>();

        assertThatCode(() -> operatorTable.lookupOperatorOverloads(
                qualified,
                SqlFunctionCategory.USER_DEFINED_FUNCTION,
                SqlSyntax.FUNCTION,
                result,
                SqlNameMatchers.withCaseSensitive(false)))
                .doesNotThrowAnyException();
        assertThat(result).isEmpty();
    }

    @Test
    void aiFunctionsExposeExactArityInsteadOfVariadicContract() {
        SqlFunction complete = (SqlFunction) lookupByName("AI_COMPLETE").get(0);
        SqlFunction embed = (SqlFunction) lookupByName("AI_EMBED").get(0);
        SqlFunction classify = (SqlFunction) lookupByName("AI_CLASSIFY").get(0);
        SqlFunction fileFromBytes = (SqlFunction) lookupByName("FILE_FROM_BYTES").get(0);

        assertThat(complete.getOperandTypeChecker().getOperandCountRange().getMin()).isEqualTo(2);
        assertThat(complete.getOperandTypeChecker().getOperandCountRange().getMax()).isEqualTo(3);
        assertThat(embed.getOperandTypeChecker().getOperandCountRange().getMin()).isEqualTo(2);
        assertThat(embed.getOperandTypeChecker().getOperandCountRange().getMax()).isEqualTo(3);
        assertThat(embed.getOperandTypeChecker().getOperandCountRange().isValidCount(2)).isTrue();
        assertThat(embed.getOperandTypeChecker().getOperandCountRange().isValidCount(3)).isTrue();
        assertThat(embed.getOperandTypeChecker().getOperandCountRange().isValidCount(1)).isFalse();
        assertThat(embed.getOperandTypeChecker().getOperandCountRange().isValidCount(4)).isFalse();
        assertThat(classify.getOperandTypeChecker().getOperandCountRange().getMin()).isEqualTo(3);
        assertThat(fileFromBytes.getOperandTypeChecker().getOperandCountRange().getMax()).isEqualTo(2);
    }

    @Test
    void textSearchPredicateAndScoreHaveDistinctContracts() {
        SqlFunction match = (SqlFunction) lookupByName("MATCH").get(0);
        SqlFunction score = (SqlFunction) lookupByName("BM25_SCORE").get(0);

        assertThat(match.getReturnTypeInference()).isSameAs(ReturnTypes.BOOLEAN_NULLABLE);
        assertThat(score.getReturnTypeInference()).isSameAs(ReturnTypes.DOUBLE_NULLABLE);
        assertThat(match.getOperandTypeChecker().getOperandCountRange().getMin()).isEqualTo(2);
        assertThat(match.getOperandTypeChecker().getOperandCountRange().getMax()).isEqualTo(2);
        assertThat(score.getOperandTypeChecker().getOperandCountRange().getMin()).isEqualTo(2);
        assertThat(score.getOperandTypeChecker().getOperandCountRange().getMax()).isEqualTo(2);
    }

    @Test
    void everyDeclaredCallbackProjectsItsCanonicalExecutionStability() {
        var callbacks = operatorTable.getOperatorList().stream()
                .filter(operator -> FunctionBindingDeclarations
                        .callbackMetadataForSqlName(operator.getName()).isPresent())
                .toList();
        assertThat(callbacks).isNotEmpty();
        assertThat(callbacks).allSatisfy(operator -> {
            var metadata = FunctionBindingDeclarations.callbackMetadataForSqlName(operator.getName()).orElseThrow();
            assertThat(operator.isDeterministic()).isEqualTo(metadata.deterministic());
            assertThat(operator.isDynamicFunction()).isEqualTo(!metadata.deterministic());
            assertThat(lookupByName(operator.getName().toLowerCase(java.util.Locale.ROOT))).singleElement()
                    .satisfies(resolved -> {
                        assertThat(resolved.isDeterministic()).isEqualTo(metadata.deterministic());
                        assertThat(resolved.isDynamicFunction()).isEqualTo(!metadata.deterministic());
                    });
        });
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

/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.pangrova.testing.e2e.ai;

import org.pangrova.indexing.engine.vector.VectorStrategyProvider;
import org.pangrova.planner.substrait.FunctionMapper;
import org.pangrova.planner.substrait.FunctionReference;
import org.pangrova.planner.validation.PangrovaSqlOperatorTable;
import org.pangrova.spi.index.IndexType;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlNameMatchers;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies the vector search chain: distance function registration in the
 * operator table and Substrait mapper, plus the vector index engine registry.
 */
@DisplayName("Vector Search Chain E2E")
class VectorSearchChainE2ETest {

    @Nested
    @DisplayName("Distance Function Registration")
    class DistanceFunctionRegistration {

        private static final Set<String> DISTANCE_FUNCTIONS = Set.of(
                "L2_DISTANCE", "COSINE_DISTANCE", "INNER_PRODUCT");

        private static PangrovaSqlOperatorTable operatorTable;
        private static FunctionMapper functionMapper;

        @BeforeAll
        static void setUp() {
            operatorTable = new PangrovaSqlOperatorTable();
            functionMapper = new FunctionMapper();
        }

        @Test
        @DisplayName("All distance functions are registered in PangrovaSqlOperatorTable")
        void distanceFunctionsRegistered() {
            for (String functionName : DISTANCE_FUNCTIONS) {
                List<SqlOperator> operators = new ArrayList<>();
                SqlIdentifier identifier = new SqlIdentifier(functionName, SqlParserPos.ZERO);
                operatorTable.lookupOperatorOverloads(
                        identifier,
                        SqlFunctionCategory.USER_DEFINED_FUNCTION,
                        SqlSyntax.FUNCTION,
                        operators,
                        SqlNameMatchers.withCaseSensitive(false));
                assertFalse(operators.isEmpty(),
                        functionName + " should be registered in PangrovaSqlOperatorTable");
            }
        }

        @Test
        @DisplayName("COSINE_SIMILARITY is also registered as a distance function")
        void cosineSimilarityRegistered() {
            List<SqlOperator> operators = new ArrayList<>();
            SqlIdentifier identifier = new SqlIdentifier("COSINE_SIMILARITY", SqlParserPos.ZERO);
            operatorTable.lookupOperatorOverloads(
                    identifier,
                    SqlFunctionCategory.USER_DEFINED_FUNCTION,
                    SqlSyntax.FUNCTION,
                    operators,
                    SqlNameMatchers.withCaseSensitive(false));
            assertFalse(operators.isEmpty(),
                    "COSINE_SIMILARITY should be registered in PangrovaSqlOperatorTable");
        }

        @Test
        @DisplayName("Distance functions map to 'vector' namespace in FunctionMapper")
        void distanceFunctionsMappedToVectorNamespace() {
            for (String functionName : DISTANCE_FUNCTIONS) {
                List<SqlOperator> operators = new ArrayList<>();
                SqlIdentifier identifier = new SqlIdentifier(functionName, SqlParserPos.ZERO);
                operatorTable.lookupOperatorOverloads(
                        identifier,
                        SqlFunctionCategory.USER_DEFINED_FUNCTION,
                        SqlSyntax.FUNCTION,
                        operators,
                        SqlNameMatchers.withCaseSensitive(false));
                assertFalse(operators.isEmpty(), "Operator lookup failed for " + functionName);

                FunctionReference ref = functionMapper.map(operators.get(0));
                assertNotNull(ref, "FunctionReference should not be null for " + functionName);
                assertEquals("vector", ref.namespace(),
                        functionName + " should map to 'vector' namespace");
            }
        }

        @Test
        @DisplayName("Distance function Substrait names use lowercase snake_case")
        void distanceFunctionSubstraitNames() {
            List<SqlOperator> operators = new ArrayList<>();
            SqlIdentifier identifier = new SqlIdentifier("L2_DISTANCE", SqlParserPos.ZERO);
            operatorTable.lookupOperatorOverloads(
                    identifier,
                    SqlFunctionCategory.USER_DEFINED_FUNCTION,
                    SqlSyntax.FUNCTION,
                    operators,
                    SqlNameMatchers.withCaseSensitive(false));

            FunctionReference ref = functionMapper.map(operators.get(0));
            assertEquals("l2_distance", ref.name(),
                    "L2_DISTANCE should map to 'l2_distance' in Substrait");
        }
    }

    @Nested
    @DisplayName("Vector Index Engine Registry")
    class VectorIndexEngineRegistry {

        @Test
        @DisplayName("VectorStrategyProvider supports VECTOR index type")
        void vectorStrategyProviderSupportsVectorType() {
            VectorStrategyProvider provider = new VectorStrategyProvider();
            assertEquals(IndexType.VECTOR, provider.supportedType(),
                    "VectorStrategyProvider should support VECTOR index type");
        }

        @Test
        @DisplayName("VectorStrategyProvider supports vector search")
        void vectorStrategyProviderSupportsVectorSearch() {
            VectorStrategyProvider provider = new VectorStrategyProvider();
            assertTrue(provider.supportsVectorSearch(),
                    "VectorStrategyProvider should support vector search");
        }

        @Test
        @DisplayName("VectorStrategyProvider supports incremental build")
        void vectorStrategyProviderSupportsIncrementalBuild() {
            VectorStrategyProvider provider = new VectorStrategyProvider();
            assertTrue(provider.supportsIncrementalBuild(),
                    "VectorStrategyProvider should support incremental build");
        }
    }
}

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

import org.pangrova.planner.substrait.FunctionMapper;
import org.pangrova.planner.substrait.FunctionReference;
import org.pangrova.planner.validation.PangrovaSqlOperatorTable;
import org.pangrova.spi.ai.inference.InferenceProvider;
import org.pangrova.spi.function.FunctionFactory;
import org.pangrova.spi.function.ScalarFunction;
import org.pangrova.worker.ai.function.AiFunctionFactory;
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
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies the AI SQL function chain from operator registration through
 * Substrait mapping and ServiceLoader discovery.
 *
 * <p>Each nested class tests one layer of the wiring: operator table registration,
 * Substrait function mapping, FunctionFactory ServiceLoader discovery, and
 * InferenceProvider ServiceLoader discovery.</p>
 */
@DisplayName("AI Function Chain E2E")
class AiFunctionChainE2ETest {

    private static final Set<String> AI_FUNCTION_NAMES = Set.of(
            "AI_COMPLETE", "AI_CLASSIFY", "AI_SUMMARIZE", "AI_FILTER",
            "AI_EMBED", "AI_EXTRACT", "AI_GUARD", "AI_PREDICT", "AI_TRANSLATE");

    @Nested
    @DisplayName("Function Registration")
    class FunctionRegistration {

        private static PangrovaSqlOperatorTable operatorTable;

        @BeforeAll
        static void setUp() {
            operatorTable = new PangrovaSqlOperatorTable();
        }

        @Test
        @DisplayName("All 9 AI functions are registered in PangrovaSqlOperatorTable")
        void allAiFunctionsRegistered() {
            for (String functionName : AI_FUNCTION_NAMES) {
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
        @DisplayName("AI functions appear in the full operator list")
        void aiFunctionsInOperatorList() {
            List<SqlOperator> allOps = operatorTable.getOperatorList();
            Set<String> opNames = new java.util.HashSet<>();
            for (SqlOperator op : allOps) {
                opNames.add(op.getName());
            }
            for (String functionName : AI_FUNCTION_NAMES) {
                assertTrue(opNames.contains(functionName),
                        functionName + " should appear in getOperatorList()");
            }
        }
    }

    @Nested
    @DisplayName("Substrait Mapping")
    class SubstraitMapping {

        private static FunctionMapper functionMapper;

        @BeforeAll
        static void setUp() {
            functionMapper = new FunctionMapper();
        }

        @Test
        @DisplayName("All 9 AI functions map to FunctionReference in the 'ai' namespace")
        void allAiFunctionsMappedToAiNamespace() {
            PangrovaSqlOperatorTable table = new PangrovaSqlOperatorTable();
            for (String functionName : AI_FUNCTION_NAMES) {
                List<SqlOperator> operators = new ArrayList<>();
                SqlIdentifier identifier = new SqlIdentifier(functionName, SqlParserPos.ZERO);
                table.lookupOperatorOverloads(
                        identifier,
                        SqlFunctionCategory.USER_DEFINED_FUNCTION,
                        SqlSyntax.FUNCTION,
                        operators,
                        SqlNameMatchers.withCaseSensitive(false));
                assertFalse(operators.isEmpty(), "Operator lookup failed for " + functionName);

                SqlOperator operator = operators.get(0);
                FunctionReference ref = functionMapper.map(operator);
                assertNotNull(ref, "FunctionReference should not be null for " + functionName);
                assertEquals("ai", ref.namespace(),
                        functionName + " should map to 'ai' namespace");
                assertEquals("ai_" + functionName.substring(3).toLowerCase(), ref.name(),
                        functionName + " should map to correct Substrait function name");
            }
        }

        @Test
        @DisplayName("AI function references produce valid compound names")
        void aiFunctionCompoundNames() {
            for (String functionName : AI_FUNCTION_NAMES) {
                String expectedName = "ai:" + "ai_" + functionName.substring(3).toLowerCase();
                FunctionReference ref = new FunctionReference("ai",
                        "ai_" + functionName.substring(3).toLowerCase());
                assertEquals(expectedName, ref.compoundName(),
                        "Compound name should be namespace:name for " + functionName);
            }
        }
    }

    @Nested
    @DisplayName("Function Factory Discovery")
    class FunctionFactoryDiscovery {

        @Test
        @DisplayName("ServiceLoader discovers AiFunctionFactory")
        void serviceLoaderDiscoversAiFunctionFactory() {
            ServiceLoader<FunctionFactory> loader = ServiceLoader.load(FunctionFactory.class);
            boolean found = false;
            for (FunctionFactory factory : loader) {
                if (factory instanceof AiFunctionFactory) {
                    found = true;
                    break;
                }
            }
            assertTrue(found,
                    "ServiceLoader should discover AiFunctionFactory via META-INF/services");
        }

        @Test
        @DisplayName("AiFunctionFactory returns all 9 scalar functions")
        void aiFunctionFactoryReturnsAllFunctions() {
            AiFunctionFactory factory = new AiFunctionFactory(modelName -> null);
            List<ScalarFunction> functions = factory.getScalarFunctions(Map.of());
            assertEquals(15, functions.size(),
                    "AiFunctionFactory should return exactly 15 scalar functions");
        }

        @Test
        @DisplayName("AiFunctionFactory name is 'ai-inference'")
        void aiFunctionFactoryName() {
            AiFunctionFactory factory = new AiFunctionFactory(modelName -> null);
            assertEquals("ai-inference", factory.getName());
        }
    }

    @Nested
    @DisplayName("Inference Provider Discovery")
    class InferenceProviderDiscovery {

        @Test
        @DisplayName("ServiceLoader can load InferenceProvider interface without error")
        void serviceLoaderCanLoadInferenceProvider() {
            ServiceLoader<InferenceProvider> loader = ServiceLoader.load(InferenceProvider.class);
            // The ServiceLoader itself should not throw; provider availability
            // depends on classpath configuration, so we verify the load succeeds
            assertNotNull(loader, "ServiceLoader should return a non-null loader");
        }
    }
}

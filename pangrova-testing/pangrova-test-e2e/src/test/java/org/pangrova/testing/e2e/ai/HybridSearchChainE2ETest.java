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

import org.pangrova.parser.ast.expression.RerankClause;
import org.pangrova.parser.ast.expression.RerankClause.CascadeStrategy;
import org.pangrova.parser.ast.expression.RerankClause.CrossEncoderStrategy;
import org.pangrova.parser.ast.expression.RerankClause.LinearCombinationStrategy;
import org.pangrova.parser.ast.expression.RerankClause.RerankStrategy;
import org.pangrova.parser.ast.expression.RerankClause.RrfStrategy;
import org.pangrova.spi.ai.rerank.RerankerFactory;
import org.pangrova.worker.ai.rerank.CascadeRerankerFactory;
import org.pangrova.worker.ai.rerank.CrossEncoderRerankerFactory;
import org.pangrova.worker.ai.rerank.RrfRerankerFactory;
import org.pangrova.worker.ai.rerank.WeightedRerankerFactory;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.ServiceLoader;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies the hybrid search chain: reranker factory discovery via ServiceLoader
 * and RERANK BY clause AST structure in the parser.
 */
@DisplayName("Hybrid Search Chain E2E")
class HybridSearchChainE2ETest {

    @Nested
    @DisplayName("Reranker Factory Discovery")
    class RerankerFactoryDiscovery {

        private static final Set<String> EXPECTED_FACTORY_CLASSES = Set.of(
                RrfRerankerFactory.class.getName(),
                WeightedRerankerFactory.class.getName(),
                CrossEncoderRerankerFactory.class.getName(),
                CascadeRerankerFactory.class.getName());

        @Test
        @DisplayName("ServiceLoader discovers all 4 RerankerFactory implementations")
        void serviceLoaderDiscoversAllFactories() {
            ServiceLoader<RerankerFactory> loader = ServiceLoader.load(RerankerFactory.class);

            Set<String> discoveredClasses = new HashSet<>();
            for (RerankerFactory factory : loader) {
                discoveredClasses.add(factory.getClass().getName());
            }

            for (String expected : EXPECTED_FACTORY_CLASSES) {
                assertTrue(discoveredClasses.contains(expected),
                        expected + " should be discoverable via ServiceLoader");
            }
        }

        @Test
        @DisplayName("Each RerankerFactory has a non-null name")
        void eachFactoryHasName() {
            ServiceLoader<RerankerFactory> loader = ServiceLoader.load(RerankerFactory.class);

            for (RerankerFactory factory : loader) {
                assertNotNull(factory.name(),
                        factory.getClass().getSimpleName() + " should have a non-null name");
                assertTrue(factory.name().length() > 0,
                        factory.getClass().getSimpleName() + " should have a non-empty name");
            }
        }

        @Test
        @DisplayName("Factory names are distinct")
        void factoryNamesAreDistinct() {
            ServiceLoader<RerankerFactory> loader = ServiceLoader.load(RerankerFactory.class);

            Set<String> names = new HashSet<>();
            for (RerankerFactory factory : loader) {
                boolean added = names.add(factory.name());
                assertTrue(added,
                        "Duplicate factory name detected: " + factory.name());
            }
        }
    }

    @Nested
    @DisplayName("Rerank By Parsing")
    class RerankByParsing {

        @Test
        @DisplayName("RerankClause can be constructed with RrfStrategy")
        void rerankClauseWithRrfStrategy() {
            RrfStrategy rrf = new RrfStrategy(null);
            RerankClause clause = new RerankClause(rrf);

            assertNotNull(clause.strategy());
            assertTrue(clause.strategy() instanceof RrfStrategy);
            assertTrue(clause.onColumns().isEmpty(),
                    "Default onColumns should be empty");
        }

        @Test
        @DisplayName("RerankClause can be constructed with LinearCombinationStrategy")
        void rerankClauseWithLinearCombinationStrategy() {
            LinearCombinationStrategy linear = new LinearCombinationStrategy(null, null);
            RerankClause clause = new RerankClause(linear);

            assertNotNull(clause.strategy());
            assertTrue(clause.strategy() instanceof LinearCombinationStrategy);
        }

        @Test
        @DisplayName("RerankClause can be constructed with CrossEncoderStrategy")
        void rerankClauseWithCrossEncoderStrategy() {
            CrossEncoderStrategy crossEncoder = new CrossEncoderStrategy("my-model", null);
            RerankClause clause = new RerankClause(crossEncoder);

            assertNotNull(clause.strategy());
            assertTrue(clause.strategy() instanceof CrossEncoderStrategy);
            assertEquals("my-model", ((CrossEncoderStrategy) clause.strategy()).modelName());
        }

        @Test
        @DisplayName("RerankClause can be constructed with CascadeStrategy")
        void rerankClauseWithCascadeStrategy() {
            RrfStrategy rrf = new RrfStrategy(null);
            CrossEncoderStrategy crossEncoder = new CrossEncoderStrategy("reranker-v2", null);
            CascadeStrategy cascade = new CascadeStrategy(List.of(rrf, crossEncoder));
            RerankClause clause = new RerankClause(cascade);

            assertNotNull(clause.strategy());
            assertTrue(clause.strategy() instanceof CascadeStrategy);
            assertEquals(2, ((CascadeStrategy) clause.strategy()).stages().size(),
                    "Cascade strategy should have 2 stages");
        }

        @Test
        @DisplayName("RerankStrategy is sealed with exactly 4 permitted subtypes")
        void rerankStrategyIsSealedWithFourPermits() {
            // Verify that RerankStrategy is sealed by checking its permitted subclasses
            assertTrue(RerankStrategy.class.isSealed(),
                    "RerankStrategy should be a sealed interface");
            Class<?>[] permitted = RerankStrategy.class.getPermittedSubclasses();
            assertNotNull(permitted);
            assertEquals(4, permitted.length,
                    "RerankStrategy should have exactly 4 permitted subtypes");
        }
    }
}

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
package org.pangrova.spi.ai.rerank;

import java.util.Map;

import org.pangrova.spi.annotation.Experimental;

/**
 * Factory for creating {@link Reranker} instances with the given configuration properties.
 *
 * <p>Implementations are discovered via {@link java.util.ServiceLoader}. Each factory
 * is identified by its {@link #name()} and creates reranker instances configured with
 * strategy-specific properties (e.g., RRF k-parameter, model endpoint, weights).</p>
 *
 * @see Reranker
 */
@Experimental
public interface RerankerFactory {

    /**
     * Returns the unique name identifying this factory (e.g., "rrf", "cross_encoder").
     *
     * @return the factory name, never null
     */
    String name();

    /**
     * Creates a new reranker instance with the given configuration properties.
     *
     * @param properties strategy-specific configuration (must not be null)
     * @return a configured reranker instance, never null
     * @throws IllegalArgumentException if properties is null or contains invalid configuration
     */
    Reranker create(Map<String, String> properties);
}

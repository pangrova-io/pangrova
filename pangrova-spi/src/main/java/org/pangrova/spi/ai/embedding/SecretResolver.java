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
package org.pangrova.spi.ai.embedding;

import org.pangrova.spi.annotation.Internal;
import org.pangrova.spi.annotation.ThreadSafe;

/**
 * SPI contract for resolving external secret references at runtime.
 *
 * <p>Embedding model configurations store API keys as opaque references
 * (e.g., {@code vault://secrets/api-key} or {@code env://API_KEY}).
 * Implementations resolve these references to actual secret values
 * without exposing credentials in catalog metadata.</p>
 */
@Internal
@ThreadSafe
public interface SecretResolver {

    /**
     * Resolves a secret reference to its actual value.
     *
     * @param secretReference the opaque reference (e.g., vault://path or env://VAR; must not be null or blank)
     * @return the resolved secret value (never null)
     * @throws IllegalArgumentException if secretReference is null or blank
     * @throws IllegalStateException    if the secret cannot be resolved (e.g., vault unreachable, key not found)
     */
    String resolve(String secretReference);
}

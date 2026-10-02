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
package org.pangrova.worker.ai.provider;

import org.pangrova.spi.ai.embedding.SecretResolver;

import java.util.Objects;
import java.util.function.Function;

/** Fail-closed built-in resolver for {@code env://VARIABLE} secret references. */
public final class EnvironmentSecretResolver implements SecretResolver {

    private static final String PREFIX = "env://";

    private final Function<String, String> environment;

    public EnvironmentSecretResolver() {
        this(System::getenv);
    }

    EnvironmentSecretResolver(Function<String, String> environment) {
        this.environment = Objects.requireNonNull(environment, "environment required");
    }

    @Override
    public String resolve(String secretReference) {
        if (secretReference == null || !secretReference.startsWith(PREFIX)) {
            throw new IllegalArgumentException(
                    "Built-in Worker secret resolver accepts only env:// references");
        }
        String variable = secretReference.substring(PREFIX.length());
        if (!variable.matches("[A-Za-z_][A-Za-z0-9_]*")) {
            throw new IllegalArgumentException(
                    "Invalid environment variable in secret reference: " + secretReference);
        }
        String value = environment.apply(variable);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException(
                    "Environment variable is missing or empty for secret reference: "
                            + secretReference);
        }
        return value;
    }
}

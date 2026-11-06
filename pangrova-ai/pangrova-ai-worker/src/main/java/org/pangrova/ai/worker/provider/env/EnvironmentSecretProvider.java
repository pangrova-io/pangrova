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
package org.pangrova.ai.worker.provider.env;

import org.pangrova.security.spi.secret.SecretProvider;
import org.pangrova.security.api.secret.SecretReference;

import java.util.Objects;
import java.util.function.Function;
import java.util.regex.Pattern;

/** Fail-closed built-in provider for {@code env://VARIABLE} secret references. */
public final class EnvironmentSecretProvider implements SecretProvider {

    private static final String PREFIX = "ENV";
    private static final Pattern VARIABLE = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

    private final Function<String, String> environment;

    public EnvironmentSecretProvider() {
        this(System::getenv);
    }

    EnvironmentSecretProvider(Function<String, String> environment) {
        this.environment = Objects.requireNonNull(environment, "environment required");
    }

    @Override
    public String prefix() {
        return PREFIX;
    }

    @Override
    public String resolve(SecretReference reference) {
        if (!PREFIX.equals(reference.prefix())) {
            throw new IllegalArgumentException(
                    "Built-in Worker secret resolver accepts only env:// references");
        }
        String variable = reference.key();
        if (!VARIABLE.matcher(variable).matches()) {
            throw new IllegalArgumentException(
                    "Invalid environment variable in secret reference: env://" + variable);
        }
        String value = environment.apply(variable);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException(
                    "Environment variable is missing or empty for secret reference: env://"
                            + variable);
        }
        return value;
    }
}

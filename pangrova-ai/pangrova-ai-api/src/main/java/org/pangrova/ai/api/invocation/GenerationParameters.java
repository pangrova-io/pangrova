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
package org.pangrova.ai.api.invocation;

import org.pangrova.common.api.annotation.Experimental;

import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.Optional;

/**
 * Typed generation parameters for LLM inference calls.
 *
 * <p>Replaces stringly-typed {@code Map<String, String>} parameter bags with
 * explicit, validated fields. All fields are optional — absent values indicate
 * the provider should use its own defaults.</p>
 *
 * @param temperature    sampling temperature (0.0 = deterministic, 2.0 = maximum randomness)
 * @param maxTokens      maximum tokens to generate in the response
 * @param responseFormat response format hint (e.g., "json_object" for structured output)
 */
@Experimental
public record GenerationParameters(
        OptionalDouble temperature,
        OptionalInt maxTokens,
        Optional<String> responseFormat) {

    public GenerationParameters {
        temperature = temperature == null ? OptionalDouble.empty() : temperature;
        maxTokens = maxTokens == null ? OptionalInt.empty() : maxTokens;
        responseFormat = responseFormat == null ? Optional.empty() : responseFormat;
        validateTemperature(temperature);
        validateMaxTokens(maxTokens);
    }

    private static void validateTemperature(OptionalDouble temperature) {
        if (temperature.isPresent()
                && (temperature.getAsDouble() < 0.0 || temperature.getAsDouble() > 2.0)) {
            throw new IllegalArgumentException("temperature must be in [0.0, 2.0]");
        }
    }

    private static void validateMaxTokens(OptionalInt maxTokens) {
        if (maxTokens.isPresent() && maxTokens.getAsInt() <= 0) {
            throw new IllegalArgumentException("maxTokens must be positive");
        }
    }

    /** No parameters — all provider defaults. */
    public static final GenerationParameters DEFAULTS = new GenerationParameters(
            OptionalDouble.empty(), OptionalInt.empty(), Optional.empty());

    /** Deterministic output (temperature=0). */
    public static GenerationParameters deterministic() {
        return new GenerationParameters(
                OptionalDouble.of(0.0), OptionalInt.empty(), Optional.empty());
    }

    /** Deterministic output with JSON response format. */
    public static GenerationParameters deterministicJson() {
        return new GenerationParameters(
                OptionalDouble.of(0.0), OptionalInt.empty(), Optional.of("json_object"));
    }

    /** Custom temperature only. */
    public static GenerationParameters withTemperature(double temperature) {
        return new GenerationParameters(
                OptionalDouble.of(temperature), OptionalInt.empty(), Optional.empty());
    }

    /** JSON response format with default temperature. */
    public static GenerationParameters jsonFormat() {
        return new GenerationParameters(
                OptionalDouble.empty(), OptionalInt.empty(), Optional.of("json_object"));
    }
}

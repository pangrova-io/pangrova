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

import org.pangrova.spi.ai.inference.GenerationParameters;
import org.pangrova.spi.ai.inference.InferenceProvider;
import org.pangrova.spi.ai.inference.InferenceResult;
import org.pangrova.spi.connector.data.ColumnValue;
import org.pangrova.worker.ai.function.AiGuardFunction;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** E2E tests for AI content safety behavior. */
@DisplayName("AI Security Governance E2E")
class AiSecurityGovernanceE2ETest {

    @Test
    @DisplayName("AiGuard returns PASS for safe content")
    void guardPassesSafeContent() {
        InferenceProvider provider = new StubInferenceProvider("PASS: content is appropriate");
        AiGuardFunction guard = new AiGuardFunction(model -> provider);

        ColumnValue result = guard.invoke(List.of(
                new ColumnValue.StringValue("tinyllama"),
                new ColumnValue.StringValue("Hello, how are you today?"),
                new ColumnValue.StringValue("no hate speech")));

        assertThat(result).isInstanceOf(ColumnValue.StringValue.class);
        assertThat(((ColumnValue.StringValue) result).value()).contains("PASS");
    }

    @Test
    @DisplayName("AiGuard returns FAIL for unsafe content")
    void guardFailsUnsafeContent() {
        InferenceProvider provider = new StubInferenceProvider("FAIL: contains offensive language");
        AiGuardFunction guard = new AiGuardFunction(model -> provider);

        ColumnValue result = guard.invoke(List.of(
                new ColumnValue.StringValue("tinyllama"),
                new ColumnValue.StringValue("some offensive text"),
                new ColumnValue.StringValue("no offensive language")));

        assertThat(result).isInstanceOf(ColumnValue.StringValue.class);
        assertThat(((ColumnValue.StringValue) result).value()).contains("FAIL");
    }

    @Test
    @DisplayName("AiGuard returns NullValue for null input text")
    void guardNullInputReturnsNull() {
        InferenceProvider provider = new StubInferenceProvider("should not be called");
        AiGuardFunction guard = new AiGuardFunction(model -> provider);

        ColumnValue result = guard.invoke(List.of(
                new ColumnValue.StringValue("tinyllama"),
                new ColumnValue.NullValue(),
                new ColumnValue.StringValue("no spam")));

        assertThat(result).isInstanceOf(ColumnValue.NullValue.class);
    }

    private static final class StubInferenceProvider implements InferenceProvider {
        private final String response;

        private StubInferenceProvider(String response) {
            this.response = response;
        }

        @Override
        public InferenceResult complete(String prompt, GenerationParameters parameters) {
            return InferenceResult.of(response);
        }

        @Override
        public List<InferenceResult> completeBatch(
                List<String> prompts, GenerationParameters parameters) {
            return prompts.stream().map(prompt -> InferenceResult.of(response)).toList();
        }

        @Override
        public String modelName() {
            return "stub";
        }

        @Override
        public int maxBatchSize() {
            return 16;
        }
    }
}
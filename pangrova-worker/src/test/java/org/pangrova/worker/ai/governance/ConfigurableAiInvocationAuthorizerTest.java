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
package org.pangrova.worker.ai.governance;

import org.junit.jupiter.api.Test;
import org.pangrova.spi.ai.AiCapabilityDescriptor;
import org.pangrova.spi.ai.AiInvocationException;
import org.pangrova.spi.ai.AiInvocationRequest;
import org.pangrova.spi.ai.inference.GenerationParameters;
import org.pangrova.spi.ai.Modality;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ConfigurableAiInvocationAuthorizerTest {

    @Test
    void discoveryWithoutAnInstalledPolicyRetainsFailClosedDefault() {
        var authorizer = ConfigurableAiInvocationAuthorizer.discover(Map.of());
        assertThatThrownBy(() -> authorizer.authorize(request("model-a", List.of(text("x"))), capability()))
                .isInstanceOf(AiInvocationException.class)
                .extracting("category")
                .isEqualTo(AiInvocationException.Category.AUTHORIZATION);
    }

    @Test
    void defaultPolicyDenies() {
        var authorizer = ConfigurableAiInvocationAuthorizer.from(Map.of());

        assertThatThrownBy(() -> authorizer.authorize(request("model-a", List.of(text("x"))), capability()))
                .isInstanceOf(AiInvocationException.class)
                .extracting("category")
                .isEqualTo(AiInvocationException.Category.AUTHORIZATION);
    }

    @Test
    void authenticatedPolicyRequiresAllExplicitAllowlists() {
        assertThatThrownBy(() -> ConfigurableAiInvocationAuthorizer.from(Map.of(
                "authorization.mode", "authenticated")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("explicit model, capability");
    }

    @Test
    void admitsExactModelCapabilityPermissionAndBoundedInput() {
        var authorizer = configured(4, 16);

        assertThatCode(() -> authorizer.authorize(
                request("model-a", List.of(text("hello"))), capability()))
                .doesNotThrowAnyException();
    }

    @Test
    void rejectsModelPermissionAndInputBudgetBeforeDispatch() {
        var authorizer = configured(1, 4);

        assertThatThrownBy(() -> authorizer.authorize(
                request("model-b", List.of(text("x"))), capability()))
                .isInstanceOf(AiInvocationException.class)
                .extracting("category")
                .isEqualTo(AiInvocationException.Category.AUTHORIZATION);
        assertThatThrownBy(() -> authorizer.authorize(
                request("model-a", List.of(text("hello"))), capability()))
                .isInstanceOf(AiInvocationException.class)
                .extracting("category")
                .isEqualTo(AiInvocationException.Category.BUDGET_EXCEEDED);
    }

    @Test
    void accountsRerankQueryAndDocumentBytesWithoutTypeFallback() {
        var authorizer = ConfigurableAiInvocationAuthorizer.from(Map.of(
                "authorization.mode", "authenticated",
                "authorization.allowed-models", "model-a",
                "authorization.allowed-capabilities", "rerank:model-a",
                "authorization.granted-permissions", "AI_MODEL_USAGE",
                "authorization.max-batch-size", "2",
                "authorization.max-input-bytes", "7"));
        AiCapabilityDescriptor rerank = new AiCapabilityDescriptor(
                "rerank:model-a",
                "v1",
                AiCapabilityDescriptor.Operation.RERANKING,
                List.of(new AiCapabilityDescriptor.Signature(
                        "DOUBLE", List.of("VARCHAR", "VARCHAR"))),
                Set.of(AiCapabilityDescriptor.ExecutionMode.BATCH),
                Set.of(Modality.TEXT),
                2,
                new AiCapabilityDescriptor.ResourceLimits(
                        new org.pangrova.spi.ai.AiRequestConstraints(8192, 0, 262144, 1048576), 0, 2, 1024, 4096),
                false,
                AiCapabilityDescriptor.RetryPolicy.noRetry(),
                Set.of("AI_MODEL_USAGE"));
        AiInvocationRequest rerankRequest = request(
                "model-a",
                AiCapabilityDescriptor.Operation.RERANKING,
                List.of(new AiInvocationRequest.RerankInput("abc", "defg")));

        assertThatCode(() -> authorizer.authorize(rerankRequest, rerank))
                .doesNotThrowAnyException();
    }

    private static ConfigurableAiInvocationAuthorizer configured(
            int maxBatch, long maxBytes) {
        return ConfigurableAiInvocationAuthorizer.from(Map.of(
                "authorization.mode", "authenticated",
                "authorization.allowed-models", "model-a",
                "authorization.allowed-capabilities", "inference:model-a",
                "authorization.granted-permissions", "AI_MODEL_USAGE",
                "authorization.max-batch-size", Integer.toString(maxBatch),
                "authorization.max-input-bytes", Long.toString(maxBytes)));
    }

    private static AiInvocationRequest.TextInput text(String value) {
        return new AiInvocationRequest.TextInput(value);
    }

    private static AiCapabilityDescriptor capability() {
        return new AiCapabilityDescriptor(
                "inference:model-a",
                "v1",
                AiCapabilityDescriptor.Operation.TEXT_GENERATION,
                List.of(new AiCapabilityDescriptor.Signature("VARCHAR", List.of("VARCHAR"))),
                Set.of(AiCapabilityDescriptor.ExecutionMode.SCALAR),
                Set.of(Modality.TEXT),
                8,
                new AiCapabilityDescriptor.ResourceLimits(
                        new org.pangrova.spi.ai.AiRequestConstraints(8192, 4096, 262144, 1048576), 0, 2, 1024, 4096),
                false,
                AiCapabilityDescriptor.RetryPolicy.noRetry(),
                Set.of("AI_MODEL_USAGE"));
    }

    private static AiInvocationRequest request(
            String model, List<AiInvocationRequest.Input> inputs) {
        return request(
                model, AiCapabilityDescriptor.Operation.TEXT_GENERATION, inputs);
    }

    private static AiInvocationRequest request(
            String model,
            AiCapabilityDescriptor.Operation operation,
            List<AiInvocationRequest.Input> inputs) {
        return new AiInvocationRequest(
                "request-1",
                "tenant-1",
                "alice",
                "query-1",
                "trace-1",
                model,
                "v1",
                new byte[32],
                "v1",
                operation,
                AiCapabilityDescriptor.ExecutionMode.SCALAR,
                operation == AiCapabilityDescriptor.Operation.EMBEDDING
                        ? java.util.Optional.of(org.pangrova.spi.ai.embedding.EmbeddingPurpose.DOCUMENT)
                        : java.util.Optional.empty(),
                inputs,
                GenerationParameters.DEFAULTS,
                Instant.now().plus(Duration.ofMinutes(1)),
                "idempotency-1",
                Map.of());
    }
}

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
package org.pangrova.ai.worker.invocation;

import org.pangrova.ai.api.enums.AiFailureCategory;
import org.pangrova.ai.api.enums.AiExecutionMode;
import org.pangrova.ai.api.enums.AiOperation;
import org.pangrova.ai.worker.governance.observer.AiInvocationObserver;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.pangrova.ai.api.exception.AiInvocationException;
import org.pangrova.ai.api.invocation.AiInvocationRequest;
import org.pangrova.ai.api.invocation.AiInvocationResult;
import org.pangrova.ai.api.invocation.GenerationParameters;
import org.pangrova.ai.worker.context.AiExecutionContext;
import org.pangrova.ai.worker.context.BoundAiInvocation;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real registry/admission and production validators. Actual HTTP lives in integration tests. */
class AiInvocationServiceTest {
    private GovernedAiTestModels models;
    private String taskId;

    @BeforeEach
    void configureRealProviders(TestInfo info) {
        models = new GovernedAiTestModels("http://127.0.0.1:1");
        taskId = info.getTestMethod().orElseThrow().getName();
    }

    @AfterEach
    void releaseResources() {
        models.close();
    }

    @Test
    void boundsDefaultGenerationBySignedProviderEnvelope() {
        var request = request(AiOperation.TEXT_GENERATION, 1);
        var effective = AiInvocationService.boundedGenerationParameters(request,
                models.inference().capabilities().iterator().next());
        assertThat(effective.maxTokens()).hasValue(500);
        assertThat(effective.temperature()).isEmpty();
        assertThat(effective.responseFormat()).isEmpty();
    }

    @Test
    void preservesExplicitGenerationOptionsWithinEnvelope() {
        var base = request(AiOperation.TEXT_GENERATION, 1);
        var parameters = new GenerationParameters(OptionalDouble.of(0.3), OptionalInt.of(500),
                java.util.Optional.of("json_object"));
        var requested = copy(base, base.modelVersion(), base.modelSignatureSha256(), base.capabilityVersion(),
                parameters, base.deadline(), base.idempotencyKey());
        assertThat(AiInvocationService.boundedGenerationParameters(requested,
                models.inference().capabilities().iterator().next())).isEqualTo(parameters);
    }

    @Test
    void rejectsRequestedGenerationBeyondSignedEnvelopeBeforeProviderCall() {
        var base = request(AiOperation.TEXT_GENERATION, 1);
        var request = copy(base, base.modelVersion(), base.modelSignatureSha256(), base.capabilityVersion(),
                new GenerationParameters(null, OptionalInt.of(501), null), base.deadline(), base.idempotencyKey());
        assertFailure(request, AiFailureCategory.INVALID_REQUEST, "exceeds signed maxOutputTokens");
    }

    @Test
    void rejectsVersionDriftWithoutCallingProvider() {
        var base = request(AiOperation.TEXT_GENERATION, 1);
        var drifted = copy(base, "wrong-version", base.modelSignatureSha256(), base.capabilityVersion(),
                base.generationParameters(), base.deadline(), base.idempotencyKey());
        assertFailure(drifted, AiFailureCategory.MODEL_NOT_FOUND, "different public identity");
    }

    @Test
    void rejectsModelSignatureDriftWithoutCallingProvider() {
        var base = request(AiOperation.TEXT_GENERATION, 1);
        byte[] signature = base.modelSignatureSha256();
        signature[0] ^= 1;
        var drifted = copy(base, base.modelVersion(), signature, base.capabilityVersion(),
                base.generationParameters(), base.deadline(), base.idempotencyKey());
        assertFailure(drifted, AiFailureCategory.MODEL_NOT_FOUND, "different public identity");
    }

    @Test
    void rejectsCapabilityVersionDriftBeforeProviderCall() {
        var base = request(AiOperation.TEXT_GENERATION, 1);
        var drifted = copy(base, base.modelVersion(), base.modelSignatureSha256(), "wrong-capability",
                base.generationParameters(), base.deadline(), base.idempotencyKey());
        assertFailure(drifted, AiFailureCategory.CAPABILITY_MISMATCH, "No exact TEXT_GENERATION");
    }

    @Test
    void authorizationIsFailClosedBeforeProviderDispatch() {
        var request = request(AiOperation.EMBEDDING, 1);
        var denied = new AiInvocationException(AiFailureCategory.AUTHORIZATION, false, "denied");
        AtomicInteger authorizationCalls = new AtomicInteger();
        var service = models.service((value, capability) -> {
            authorizationCalls.incrementAndGet();
            throw denied;
        }, AiInvocationObserver.noop());
        assertThatThrownBy(() -> invoke(service, request)).isSameAs(denied);
        assertThat(authorizationCalls).hasValue(1);
    }

    @Test
    void observerFailureDoesNotReplacePrimaryAuthorizationFailure() {
        var denied = new AiInvocationException(AiFailureCategory.AUTHORIZATION, false, "denied");
        var auditFailure = new IllegalStateException("audit unavailable");
        var service = models.service((value, capability) -> { throw denied; },
                new AiInvocationObserver() {
                    @Override public void onSuccess(AiInvocationRequest request, AiInvocationResult result) {}
                    @Override public void onFailure(AiInvocationRequest request, AiInvocationException failure) {
                        throw auditFailure;
                    }
                });
        assertThatThrownBy(() -> invoke(service, request(AiOperation.EMBEDDING, 1)))
                .isSameAs(denied).satisfies(failure ->
                        assertThat(failure.getSuppressed()).containsExactly(auditFailure));
    }

    @Test
    void rejectsExpiredInvocationBeforeProviderResolution() {
        var base = request(AiOperation.TEXT_GENERATION, 1);
        var expired = copy(base, base.modelVersion(), base.modelSignatureSha256(), base.capabilityVersion(),
                base.generationParameters(), Instant.EPOCH, base.idempotencyKey());
        assertFailure(expired, AiFailureCategory.TIMEOUT, "deadline has expired");
    }

    @Test
    void rejectsOversizedBatchBeforeProviderCall() {
        assertFailure(request(AiOperation.EMBEDDING, 9),
                AiFailureCategory.INVALID_REQUEST, "exceeds capability maxBatchSize 8");
    }

    @Test
    void rejectsUnsupportedTextExecutionModeBeforeProviderCall() {
        var request = models.request(AiOperation.TEXT_GENERATION, taskId,
                AiExecutionMode.ASYNC, List.of(new AiInvocationRequest.TextInput("one")));
        assertFailure(request, AiFailureCategory.CAPABILITY_MISMATCH,
                "does not support execution mode ASYNC");
    }

    @Test
    void textBatchRejectsInsufficientConcurrentProviderGrant() {
        var request = request(AiOperation.TEXT_GENERATION, 2);
        var original = models.context(taskId);
        var context = envelope(original, 1, 8, 10000, 2000);
        assertThatThrownBy(() -> invoke(service(), request, context))
                .isInstanceOfSatisfying(AiInvocationException.class, failure ->
                        assertThat(failure.category()).isEqualTo(AiFailureCategory.AUTHORIZATION));
    }

    @Test
    void rejectsCallbackGrantDifferentFromActualRegisteredTaskBeforeProviderCall() {
        var request = request(AiOperation.TEXT_GENERATION, 1);
        var context = envelope(models.context(taskId), 1, 1, 999, 500);
        assertThatThrownBy(() -> invoke(service(), request, context))
                .isInstanceOfSatisfying(AiInvocationException.class, failure ->
                        assertThat(failure.category()).isEqualTo(AiFailureCategory.AUTHORIZATION))
                .hasMessageContaining("different authority or grant");
    }

    @Test
    void rejectsRequestDeadlineDifferentFromSignedTaskDeadline() {
        var base = request(AiOperation.EMBEDDING, 1);
        var changed = copy(base, base.modelVersion(), base.modelSignatureSha256(), base.capabilityVersion(),
                base.generationParameters(), base.deadline().plusSeconds(1), base.idempotencyKey());
        assertFailure(changed, AiFailureCategory.AUTHORIZATION, "trusted native task context");
    }

    @Test
    void rejectsIdempotencyIdentityBorrowedFromAnotherOperator() {
        var base = request(AiOperation.EMBEDDING, 1);
        var changed = copy(base, base.modelVersion(), base.modelSignatureSha256(), base.capabilityVersion(),
                base.generationParameters(), base.deadline(), "another-operator:" + "0".repeat(64));
        assertFailure(changed, AiFailureCategory.AUTHORIZATION, "trusted native task context");
    }

    @Test
    void rejectsNullProviderResultUsingProductionResultBoundary() {
        assertThatThrownBy(() -> AiEmbeddingInvocationSupport.requireEmbeddingResult(null))
                .isInstanceOfSatisfying(AiInvocationException.class, failure -> {
                    assertThat(failure.category()).isEqualTo(AiFailureCategory.PROVIDER_RESPONSE_INVALID);
                    assertThat(failure.retryable()).isFalse();
                }).hasMessageContaining("returned no result");
    }

    @Test
    void embeddingRejectsNullAndNonFiniteVectorsThroughProductionClassifier() {
        var request = request(AiOperation.EMBEDDING, 1);
        for (float invalid : new float[]{Float.NaN, Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY}) {
            float[] vector = new float[384];
            vector[0] = invalid;
            assertInvalidEmbedding(request, List.of(vector));
        }
        assertInvalidEmbedding(request, java.util.Collections.singletonList(null));
        assertInvalidEmbedding(request, null);
        assertInvalidEmbedding(request, List.of(new float[383]));
    }

    @Test
    void embeddingReturnsOrderedFirstClassDefensivelyOwnedVectors() {
        var request = request(AiOperation.EMBEDDING, 2);
        float[] first = new float[384];
        float[] second = new float[384];
        Arrays.fill(first, 0.25f);
        Arrays.fill(second, 0.75f);
        var outputs = AiEmbeddingInvocationSupport.embeddingOutputs(request, models.embedding(), List.of(first, second));
        first[0] = 99;
        second[0] = 99;
        assertThat(outputs).hasSize(2);
        assertThat(((AiInvocationResult.VectorOutput) outputs.get(0)).vector()).containsOnly(0.25f);
        assertThat(((AiInvocationResult.VectorOutput) outputs.get(1)).vector()).containsOnly(0.75f);
    }

    private void assertInvalidEmbedding(AiInvocationRequest request, List<float[]> vectors) {
        assertThatThrownBy(() -> {
            try {
                AiEmbeddingInvocationSupport.embeddingOutputs(request, models.embedding(), vectors);
            } catch (RuntimeException failure) {
                throw AiInvocationFailures.classify(failure);
            }
        }).isInstanceOfSatisfying(AiInvocationException.class, failure ->
                assertThat(failure.category()).isEqualTo(AiFailureCategory.PROVIDER_RESPONSE_INVALID));
    }

    private void assertFailure(AiInvocationRequest request, AiFailureCategory category, String message) {
        assertThatThrownBy(() -> invoke(service(), request))
                .isInstanceOfSatisfying(AiInvocationException.class, failure -> {
                    assertThat(failure.category()).isEqualTo(category);
                    assertThat(failure.retryable()).isFalse();
                }).hasMessageContaining(message);
    }

    private AiInvocationService service() {
        return models.service((request, capability) -> {}, AiInvocationObserver.noop());
    }

    private AiInvocationRequest request(AiOperation operation, int count) {
        var inputs = java.util.stream.IntStream.range(0, count)
                .mapToObj(index -> (AiInvocationRequest.Input) new AiInvocationRequest.TextInput("row " + index)).toList();
        return models.request(operation, taskId, count == 1 ? AiExecutionMode.SCALAR
                : AiExecutionMode.BATCH, inputs);
    }

    private AiInvocationResult invoke(AiInvocationService service, AiInvocationRequest request) {
        return models.invoke(request.operation(), taskId, 8, () -> service.invoke(request));
    }

    private AiInvocationResult invoke(AiInvocationService service, AiInvocationRequest request,
            AiExecutionContext context) {
        var identity = models.binding(request.operation(), taskId, context.maxProviderItems());
        var binding = new BoundAiInvocation(identity.invocationId(), identity.modelId(), identity.modelVersion(),
                identity.modelSignatureSha256(), identity.operation(), identity.idempotencyBase(),
                identity.requestConstraints(), context.maxProviderConcurrency(), context.maxProviderItems(),
                context.maxProviderAttempts(), context.maxProviderRequestBytes(), context.maxProviderResponseBytes(),
                context.maxMediaBytes(), identity.outputVectorDimension());
        return AiExecutionContext.callWith(context, () ->
                AiExecutionContext.callWithBoundInvocation(binding, () -> service.invoke(request)));
    }

    private static AiExecutionContext envelope(AiExecutionContext original, int concurrency,
            long calls, long requestBytes, long responseBytes) {
        return new AiExecutionContext(original.tenantId(), original.principal(), original.queryId(),
                original.taskId(), original.traceId(), original.deadlineEpochMs(), concurrency, calls,
                original.maxProviderAttempts(), requestBytes, responseBytes, original.maxMediaBytes(), original.taskAttempt());
    }

    private static AiInvocationRequest copy(AiInvocationRequest base, String version, byte[] signature,
            String capabilityVersion, GenerationParameters parameters, Instant deadline, String idempotencyKey) {
        return new AiInvocationRequest(base.requestId(), base.tenantId(), base.principal(), base.queryId(),
                base.traceId(), base.modelName(), version, signature, capabilityVersion, base.operation(),
                base.executionMode(), base.embeddingPurpose(), base.inputs(), parameters, deadline,
                idempotencyKey, base.attributes());
    }
}

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

import org.pangrova.ai.worker.enums.EmbeddingInputKind;

import org.pangrova.ai.api.enums.AiFinishReason;
import org.pangrova.ai.api.enums.AiFailureCategory;
import org.pangrova.ai.worker.governance.observer.AiInvocationObserver;
import org.pangrova.ai.worker.governance.AiInvocationAuthorizer;

import static org.pangrova.ai.worker.invocation.AiInvocationFailures.classify;
import static org.pangrova.ai.worker.invocation.AiInvocationFailures.invalidInput;
import static org.pangrova.ai.worker.invocation.AiInvocationFailures.modelNotFound;
import static org.pangrova.ai.worker.invocation.AiInvocationResultSupport.inferenceResult;
import static org.pangrova.ai.worker.invocation.AiInvocationResultSupport.validateResultCount;
import static org.pangrova.ai.worker.invocation.AiEmbeddingInvocationSupport.blobBytes;
import static org.pangrova.ai.worker.invocation.AiEmbeddingInvocationSupport.embeddingInputKind;
import static org.pangrova.ai.worker.invocation.AiEmbeddingInvocationSupport.embeddingOutputs;
import static org.pangrova.ai.worker.invocation.AiEmbeddingInvocationSupport.invokeEmbeddingProvider;

import org.pangrova.ai.api.capability.AiCapabilityDescriptor;
import org.pangrova.ai.api.exception.AiInvocationException;
import org.pangrova.ai.api.invocation.AiInvocationRequest;
import org.pangrova.ai.api.invocation.AiInvocationResult;
import org.pangrova.ai.worker.invocation.AiEmbeddingInvocationSupport.EmbeddingResult;
import org.pangrova.ai.api.embedding.EmbeddingProvider;
import org.pangrova.ai.api.inference.InferenceProvider;
import org.pangrova.ai.api.invocation.InferenceResult;
import org.pangrova.ai.api.invocation.GenerationParameters;
import org.pangrova.ai.api.enums.Modality;
import org.pangrova.protocol.v2.TaskAttemptId;
import org.pangrova.protocol.v2.ExecutionCallbackContext;
import org.pangrova.protocol.v2.OperatorBinding;
import org.pangrova.protocol.v2.ResourceUsage;
import org.pangrova.ai.api.invocation.RerankingBatchResult;
import org.pangrova.ai.api.reranking.RerankingProvider;
import org.pangrova.ai.worker.context.AiExecutionContext;
import org.pangrova.ai.api.invocation.AiProviderCallContext;

import java.security.MessageDigest;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.Set;
import java.util.function.Function;

/**
 * Single fail-closed execution path for text, multimodal and embedding models.
 * It validates the exact model/capability version, execution mode, modality,
 * batch cardinality and dimensions before emitting one auditable result.
 */
public final class AiInvocationService {

    private final Function<String, InferenceProvider> inferenceLookup;
    private final Function<String, EmbeddingProvider> embeddingLookup;
    private final Function<String, RerankingProvider> rerankingLookup;
    private final AiInvocationAuthorizer authorizer;
    private final AiInvocationTerminalReporter terminalReporter;
    private static final AiInvocationBudgetManager BUDGET_MANAGER =
            new AiInvocationBudgetManager();

    /** Registers canonical Worker task authority before any Native callback can execute. */
    public static void registerTask(ExecutionCallbackContext callback, List<OperatorBinding> bindings) {
        BUDGET_MANAGER.registerTask(AiExecutionContext.fromCanonicalTaskAuthority(callback), bindings);
    }

    /** Returns the actual admitted binding; callback literals are never an authority. */
    public static org.pangrova.protocol.v2.OperatorBinding admittedBinding(String invocationId) {
        var context = AiExecutionContext.current().orElseThrow(() ->
                new AiInvocationException(AiFailureCategory.AUTHORIZATION, false,
                        "AI callback has no trusted task context"));
        return BUDGET_MANAGER.taskBudget(context).admittedBinding(invocationId);
    }

    /** Creates the fail-closed invocation path for all configured provider types. */
    public AiInvocationService(
            Function<String, InferenceProvider> inferenceLookup,
            Function<String, EmbeddingProvider> embeddingLookup,
            Function<String, RerankingProvider> rerankingLookup,
            AiInvocationAuthorizer authorizer,
            AiInvocationObserver observer) {
        this.inferenceLookup = Objects.requireNonNull(inferenceLookup, "inferenceLookup required");
        this.embeddingLookup = Objects.requireNonNull(embeddingLookup, "embeddingLookup required");
        this.rerankingLookup = Objects.requireNonNull(rerankingLookup, "rerankingLookup required");
        this.authorizer = Objects.requireNonNull(authorizer, "authorizer required");
        this.terminalReporter = new AiInvocationTerminalReporter(observer);
    }

    /** Retires transient provider admission state after the Native task joins. */
    public static ResourceUsage releaseTask(String tenantId, TaskAttemptId attempt) {
        return BUDGET_MANAGER.releaseTask(tenantId, attempt);
    }

    /** Revokes new sends immediately while retaining resources and evidence until joined release. */
    public static void cancelTask(String tenantId, TaskAttemptId attempt) {
        BUDGET_MANAGER.cancelTask(tenantId, attempt);
    }

    /** Kernel resource counters for the exact live task attempt, including failed transports. */
    public static ResourceUsage resourceUsage(String tenantId, TaskAttemptId attempt) {
        return BUDGET_MANAGER.usage(tenantId, attempt);
    }

    /** Charges a FILE publication before the object-store send under the current task authority. */
    public static void chargeGovernedFileBytes(long bytes) {
        var context = AiExecutionContext.current().orElseThrow(() ->
                new AiInvocationException(AiFailureCategory.AUTHORIZATION, false,
                        "FILE callback has no trusted task context"));
        BUDGET_MANAGER.chargeGovernedFileBytes(context, bytes);
    }

    /** Executes one governed invocation and reports its audited outcome. */
    public AiInvocationResult invoke(AiInvocationRequest request) {
        Objects.requireNonNull(request, "request must not be null");
        AiInvocationResult result;
        try {
            request.requireNotExpired(Instant.now());
            result = switch (request.operation()) {
                case TEXT_GENERATION -> invokeText(request);
                case MULTIMODAL_GENERATION -> invokeMultimodal(request);
                case EMBEDDING -> invokeEmbedding(request);
                case RERANKING -> invokeReranking(request);
            };
        } catch (AiInvocationException failure) {
            terminalReporter.failure(request, failure);
            throw failure;
        } catch (RuntimeException failure) {
            AiInvocationException classified = classify(failure);
            terminalReporter.failure(request, classified);
            throw classified;
        }
        terminalReporter.success(request, result);
        return result;
    }

    private AiInvocationResult invokeText(AiInvocationRequest request) {
        InferenceProvider provider = requireInferenceProvider(request);
        AiCapabilityDescriptor capability = requireCapability(provider.capabilities(), request);
        authorizeAndValidate(request, capability, Set.of(Modality.TEXT));
        GenerationParameters generationParameters = boundedGenerationParameters(
                request, capability);
        List<String> prompts = request.inputs().stream().map(input -> {
            if (input instanceof AiInvocationRequest.TextInput text) {
                return text.text();
            }
            throw invalidInput(request, "TEXT_GENERATION accepts only TextInput");
        }).toList();
        int concurrentCalls = Math.min(
                prompts.size(), capability.resourceLimits().maxConcurrency());
        try (AiPhysicalInvocationAdmission admission = admit(
                request, capability, prompts.size(), prompts.size(),
                concurrentCalls, 0)) {
            List<InferenceResult> results = prompts.size() == 1
                    ? java.util.Collections.singletonList(AiProviderCallContext.callWith(
                            request.idempotencyKey(),
                            request.deadline(),
                            admission::openAttempt,
                            () -> provider.complete(
                                    prompts.get(0),
                                    generationParameters)))
                    : AiProviderCallContext.callWith(
                            request.idempotencyKey(),
                            request.deadline(),
                            admission::openAttempt,
                            () -> provider.completeBatch(
                                    prompts, generationParameters));
            validateResultCount(request, results, prompts.size());
            return inferenceResult(request, results);
        }
    }

    private AiInvocationResult invokeMultimodal(AiInvocationRequest request) {
        InferenceProvider provider = requireInferenceProvider(request);
        AiCapabilityDescriptor capability = requireCapability(provider.capabilities(), request);
        if (request.inputs().size() != 1
                || !(request.inputs().get(0) instanceof AiInvocationRequest.MultimodalInput input)) {
            throw invalidInput(request, "MULTIMODAL_GENERATION requires one MultimodalInput");
        }
        Set<Modality> requestedModalities = input.prompt().parts().stream()
                .map(part -> modalityOf(part))
                .collect(java.util.stream.Collectors.toUnmodifiableSet());
        authorizeAndValidate(request, capability, requestedModalities);
        GenerationParameters generationParameters = boundedGenerationParameters(
                request, capability);
        long mediaBytes = mediaBytes(input.prompt());
        try (AiPhysicalInvocationAdmission admission = admit(
                request, capability, 1, 1, 1, mediaBytes)) {
            InferenceResult providerResult = AiProviderCallContext.callWith(
                    request.idempotencyKey(),
                    request.deadline(),
                    admission::openAttempt,
                    () -> provider.completeMultimodal(
                            input.prompt(), generationParameters));
            validateResultCount(request, java.util.Collections.singletonList(providerResult), 1);
            return inferenceResult(request, List.of(providerResult));
        }
    }

    private AiInvocationResult invokeEmbedding(AiInvocationRequest request) {
        EmbeddingProvider provider = requireEmbeddingProvider(request);
        AiCapabilityDescriptor capability = requireCapability(provider.capabilities(), request);
        EmbeddingInputKind inputKind = embeddingInputKind(request);
        authorizeAndValidate(
                request,
                capability,
                inputKind == EmbeddingInputKind.TEXT
                        ? Set.of(Modality.TEXT) : blobModalities(request));
        long mediaBytes = inputKind == EmbeddingInputKind.BLOB
                ? blobBytes(request) : 0L;
        try (AiPhysicalInvocationAdmission admission = admit(
                request, capability, request.inputs().size(), 1, 1, mediaBytes)) {
            EmbeddingResult embedding = invokeEmbeddingProvider(
                    request, provider, inputKind, admission);
            List<AiInvocationResult.Output> outputs = embeddingOutputs(
                    request, provider, embedding.providerResult().vectors());
            return new AiInvocationResult(
                    request.requestId(), request.modelName(), request.modelVersion(),
                    outputs, embedding.providerResult().providerRequestId(),
                    AiFinishReason.COMPLETE, java.util.Map.of());
        }
    }

    private AiInvocationResult invokeReranking(AiInvocationRequest request) {
        RerankingProvider provider = requireRerankingProvider(request);
        AiCapabilityDescriptor capability = requireCapability(
                provider.capabilities(), request);
        authorizeAndValidate(request, capability, Set.of(Modality.TEXT));
        RerankInputs inputs = rerankInputs(request);
        try (AiPhysicalInvocationAdmission admission = admit(
                request, capability, request.inputs().size(), 1, 1, 0)) {
            RerankingBatchResult providerResult = AiProviderCallContext.callWith(
                    request.idempotencyKey(),
                    request.deadline(),
                    admission::openAttempt,
                    () -> provider.score(inputs.query(), inputs.documents()));
            return rerankingResult(request, inputs, providerResult);
        }
    }

    private static RerankInputs rerankInputs(AiInvocationRequest request) {
        List<AiInvocationRequest.RerankInput> pairs = request.inputs().stream()
                .map(input -> {
                    if (input instanceof AiInvocationRequest.RerankInput pair) {
                        return pair;
                    }
                    throw invalidInput(
                            request, "RERANKING accepts only RerankInput values");
                })
                .toList();
        String query = pairs.get(0).query();
        if (pairs.stream().anyMatch(input -> !input.query().equals(query))) {
            throw invalidInput(
                    request, "one reranking batch must use one canonical query");
        }
        List<String> documents = pairs.stream()
                .map(AiInvocationRequest.RerankInput::document)
                .toList();
        return new RerankInputs(query, documents);
    }

    private static AiInvocationResult rerankingResult(
            AiInvocationRequest request,
            RerankInputs inputs,
            RerankingBatchResult providerResult) {
        if (providerResult == null) {
            throw new AiInvocationException(AiFailureCategory.PROVIDER_RESPONSE_INVALID,
                    false, "Reranking provider returned no result");
        }
        validateResultCount(
                request, providerResult.scores(), inputs.documents().size());
        List<AiInvocationResult.Output> outputs = providerResult.scores().stream()
                .map(score -> (AiInvocationResult.Output)
                        new AiInvocationResult.ScoreOutput(score))
                .toList();
        return new AiInvocationResult(
                request.requestId(), request.modelName(), request.modelVersion(),
                outputs, providerResult.providerRequestId(),
                AiFinishReason.COMPLETE, java.util.Map.of());
    }

    private AiPhysicalInvocationAdmission admit(
            AiInvocationRequest request,
            AiCapabilityDescriptor capability,
            int logicalInvocations,
            int providerRequests,
            int concurrentCalls,
            long mediaBytes) {
        AiExecutionContext context = AiExecutionContext.current().orElseThrow(() ->
                new AiInvocationException(
                        AiFailureCategory.AUTHORIZATION,
                        false,
                        "AI invocation has no trusted native task context"));
        return BUDGET_MANAGER.acquire(
                context, request, capability,
                logicalInvocations, providerRequests, concurrentCalls, mediaBytes);
    }

    private InferenceProvider requireInferenceProvider(AiInvocationRequest request) {
        InferenceProvider provider = inferenceLookup.apply(request.modelName());
        if (provider == null) {
            throw modelNotFound(request);
        }
        requireExactModel(
                request, provider.modelName(), provider.modelVersion(),
                provider.modelSignatureSha256());
        return provider;
    }

    private EmbeddingProvider requireEmbeddingProvider(AiInvocationRequest request) {
        EmbeddingProvider provider = embeddingLookup.apply(request.modelName());
        if (provider == null) {
            throw modelNotFound(request);
        }
        requireExactModel(
                request, provider.modelName(), provider.modelVersion(),
                provider.modelSignatureSha256());
        return provider;
    }

    private RerankingProvider requireRerankingProvider(
            AiInvocationRequest request) {
        RerankingProvider provider = rerankingLookup.apply(request.modelName());
        if (provider == null) {
            throw modelNotFound(request);
        }
        requireExactModel(
                request, provider.modelName(), provider.modelVersion(),
                provider.modelSignatureSha256());
        return provider;
    }

    private static void requireExactModel(
            AiInvocationRequest request,
            String actualName,
            String actualVersion,
            byte[] actualSignature) {
        if (!request.modelName().equals(actualName)
                || !request.modelVersion().equals(actualVersion)
                || actualSignature == null
                || !MessageDigest.isEqual(
                        request.modelSignatureSha256(), actualSignature)) {
            throw new AiInvocationException(
                    AiFailureCategory.MODEL_NOT_FOUND,
                    false,
                    "Requested exact model " + request.modelName() + "@" + request.modelVersion()
                            + " with its bound signature, but provider resolved "
                            + actualName + "@" + actualVersion
                            + " with a different public identity");
        }
    }

    private static AiCapabilityDescriptor requireCapability(
            Set<AiCapabilityDescriptor> capabilities, AiInvocationRequest request) {
        return capabilities.stream()
                .filter(value -> value.operation() == request.operation())
                .filter(value -> value.version().equals(request.capabilityVersion()))
                .findFirst()
                .orElseThrow(() -> new AiInvocationException(
                        AiFailureCategory.CAPABILITY_MISMATCH,
                        false,
                        "No exact " + request.operation() + " capability version '"
                                + request.capabilityVersion() + "' for model '"
                                + request.modelName() + "'"));
    }

    private void authorizeAndValidate(
            AiInvocationRequest request,
            AiCapabilityDescriptor capability,
            Set<Modality> requestedModalities) {
        if (!capability.executionModes().contains(request.executionMode())) {
            throw new AiInvocationException(
                    AiFailureCategory.CAPABILITY_MISMATCH,
                    false,
                    "Capability does not support execution mode " + request.executionMode());
        }
        if (!capability.modalities().containsAll(requestedModalities)) {
            throw new AiInvocationException(
                    AiFailureCategory.CAPABILITY_MISMATCH,
                    false,
                    "Capability does not support requested modalities " + requestedModalities);
        }
        if (request.inputs().size() > capability.maxBatchSize()) {
            throw invalidInput(request, "batch size " + request.inputs().size()
                    + " exceeds capability maxBatchSize " + capability.maxBatchSize());
        }
        authorizer.authorize(request, capability);
    }

    static GenerationParameters boundedGenerationParameters(
            AiInvocationRequest request,
            AiCapabilityDescriptor capability) {
        long signedLimit = capability.resourceLimits().requestConstraints().maxOutputTokens();
        if (signedLimit <= 0) {
            throw invalidInput(request,
                    "generation capability has no output-token allowance");
        }
        GenerationParameters requested = request.generationParameters();
        OptionalInt requestedLimit = requested.maxTokens();
        if (requestedLimit.isPresent() && requestedLimit.getAsInt() > signedLimit) {
            throw invalidInput(request,
                    "maxTokens " + requestedLimit.getAsInt()
                            + " exceeds signed maxOutputTokens " + signedLimit);
        }
        int effectiveLimit = requestedLimit.orElseGet(
                () -> (int) Math.min(signedLimit, Integer.MAX_VALUE));
        return new GenerationParameters(
                requested.temperature(), OptionalInt.of(effectiveLimit),
                requested.responseFormat());
    }

    private static Set<Modality> blobModalities(AiInvocationRequest request) {
        return request.inputs().stream()
                .map(AiInvocationRequest.BlobInput.class::cast)
                .map(AiInvocationRequest.BlobInput::reference)
                .map(reference -> modalityOf(reference.mimeType()))
                .collect(java.util.stream.Collectors.toUnmodifiableSet());
    }

    private static long mediaBytes(
            org.pangrova.ai.api.invocation.MultimodalPrompt prompt) {
        long result = 0;
        for (var part : prompt.parts()) {
            if (part instanceof org.pangrova.ai.api.invocation.ContentPart.FileRef file) {
                result = Math.addExact(result, file.reference().size());
            } else if (part instanceof org.pangrova.ai.api.invocation.ContentPart.InlineImage image) {
                result = Math.addExact(result, image.data().length);
            }
        }
        return result;
    }

    private static Modality modalityOf(org.pangrova.ai.api.invocation.ContentPart part) {
        if (part instanceof org.pangrova.ai.api.invocation.ContentPart.Text) {
            return Modality.TEXT;
        }
        if (part instanceof org.pangrova.ai.api.invocation.ContentPart.InlineImage) {
            return Modality.IMAGE;
        }
        var reference = ((org.pangrova.ai.api.invocation.ContentPart.FileRef) part).reference();
        return modalityOf(reference.mimeType());
    }

    private static Modality modalityOf(String mimeType) {
        String type = mimeType == null ? "" : mimeType.toLowerCase(java.util.Locale.ROOT);
        if (type.startsWith("image/")) return Modality.IMAGE;
        if (type.startsWith("audio/")) return Modality.AUDIO;
        if (type.startsWith("video/")) return Modality.VIDEO;
        if (type.startsWith("text/")) return Modality.TEXT;
        return Modality.DOCUMENT;
    }

    private record RerankInputs(String query, List<String> documents) {}
}

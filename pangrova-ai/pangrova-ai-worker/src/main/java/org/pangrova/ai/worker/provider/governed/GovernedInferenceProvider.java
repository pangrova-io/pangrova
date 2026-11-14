package org.pangrova.ai.worker.provider.governed;

import org.pangrova.ai.api.enums.AiExecutionMode;
import org.pangrova.ai.api.enums.AiOperation;
import org.pangrova.ai.api.capability.AiCapabilityDescriptor;
import org.pangrova.ai.api.invocation.AiInvocationRequest;
import org.pangrova.ai.api.invocation.AiInvocationResult;
import org.pangrova.ai.api.invocation.GenerationParameters;
import org.pangrova.ai.api.inference.InferenceProvider;
import org.pangrova.ai.api.invocation.InferenceResult;
import org.pangrova.ai.api.enums.Modality;
import org.pangrova.ai.api.invocation.MultimodalPrompt;
import org.pangrova.ai.worker.invocation.AiInvocationService;

import java.util.List;
import java.util.Set;

/** Inference provider facade that makes the unified invocation contract unavoidable. */
public final class GovernedInferenceProvider implements InferenceProvider {

    private final InferenceProvider delegate;
    private final AiInvocationService service;

    public GovernedInferenceProvider(InferenceProvider delegate, AiInvocationService service) {
        this.delegate = java.util.Objects.requireNonNull(delegate, "delegate must not be null");
        this.service = java.util.Objects.requireNonNull(service, "service must not be null");
    }

    @Override
    public InferenceResult complete(String prompt, GenerationParameters parameters) {
        AiCapabilityDescriptor capability = AiInvocationRequests.requireCapability(
                capabilities(), AiOperation.TEXT_GENERATION);
        var request = AiInvocationRequests.create(
                modelName(), modelVersion(), modelSignatureSha256(), capability,
                AiExecutionMode.SCALAR,
                List.of(new AiInvocationRequest.TextInput(prompt)), parameters);
        return singleResult(service.invoke(request));
    }

    @Override
    public List<InferenceResult> completeBatch(
            List<String> prompts, GenerationParameters parameters) {
        AiCapabilityDescriptor capability = AiInvocationRequests.requireCapability(
                capabilities(), AiOperation.TEXT_GENERATION);
        List<AiInvocationRequest.Input> inputs = prompts.stream()
                .map(value -> (AiInvocationRequest.Input) new AiInvocationRequest.TextInput(value))
                .toList();
        var request = AiInvocationRequests.create(
                modelName(), modelVersion(), modelSignatureSha256(), capability,
                prompts.size() == 1
                        ? AiExecutionMode.SCALAR
                        : AiExecutionMode.BATCH,
                inputs, parameters);
        AiInvocationResult result = service.invoke(request);
        return java.util.stream.IntStream.range(0, result.outputs().size())
                .mapToObj(index -> new InferenceResult(text(result.outputs().get(index)),
                        result.attributes().getOrDefault("provider_request_" + index, ""))).toList();
    }

    @Override
    public InferenceResult completeMultimodal(
            MultimodalPrompt prompt, GenerationParameters parameters) {
        AiCapabilityDescriptor capability = AiInvocationRequests.requireCapability(
                capabilities(), AiOperation.MULTIMODAL_GENERATION);
        var request = AiInvocationRequests.create(
                modelName(), modelVersion(), modelSignatureSha256(), capability,
                AiExecutionMode.SCALAR,
                List.of(new AiInvocationRequest.MultimodalInput(prompt)), parameters);
        return singleResult(service.invoke(request));
    }

    private static InferenceResult singleResult(AiInvocationResult result) {
        String value = text(result.outputs().get(0));
        return new InferenceResult(value, result.providerRequestId());
    }

    private static String text(AiInvocationResult.Output output) {
        if (output instanceof AiInvocationResult.TextOutput text) {
            return text.text();
        }
        throw new IllegalStateException("Inference invocation returned a non-text output");
    }

    @Override public String modelName() { return delegate.modelName(); }
    @Override public String modelVersion() { return delegate.modelVersion(); }
    @Override public byte[] modelSignatureSha256() {
        return delegate.modelSignatureSha256();
    }
    @Override public int maxBatchSize() { return delegate.maxBatchSize(); }
    @Override public Set<Modality> supportedModalities() { return delegate.supportedModalities(); }
    @Override public Set<AiCapabilityDescriptor> capabilities() { return delegate.capabilities(); }
}

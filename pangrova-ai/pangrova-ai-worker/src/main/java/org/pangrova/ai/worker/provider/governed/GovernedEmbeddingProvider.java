package org.pangrova.ai.worker.provider.governed;

import org.pangrova.ai.api.capability.AiCapabilityDescriptor;
import org.pangrova.ai.api.invocation.AiInvocationRequest;
import org.pangrova.ai.api.invocation.AiInvocationResult;
import org.pangrova.ai.api.embedding.EmbeddingBatchRequest;
import org.pangrova.ai.api.embedding.EmbeddingPurpose;
import org.pangrova.ai.spi.embedding.EmbeddingProvider;
import org.pangrova.ai.api.embedding.EmbeddingBatchResult;
import org.pangrova.ai.api.capability.Modality;
import org.pangrova.ai.worker.invocation.AiInvocationService;

import java.util.List;
import java.util.Set;

/** Embedding provider that enforces exact identity, authorization and task admission. */
public final class GovernedEmbeddingProvider implements EmbeddingProvider {

    private final EmbeddingProvider delegate;
    private final AiInvocationService service;

    public GovernedEmbeddingProvider(EmbeddingProvider delegate, AiInvocationService service) {
        this.delegate = java.util.Objects.requireNonNull(delegate, "delegate must not be null");
        this.service = java.util.Objects.requireNonNull(service, "service must not be null");
    }

    @Override
    public EmbeddingBatchResult embed(EmbeddingBatchRequest request) {
        java.util.Objects.requireNonNull(request, "request required");
        List<AiInvocationRequest.Input> inputs = request.inputs().stream()
                .map(GovernedEmbeddingProvider::input).toList();
        return batchResult(invoke(inputs, request.purpose()));
    }

    private static AiInvocationRequest.Input input(EmbeddingBatchRequest.Input input) {
        if (input instanceof EmbeddingBatchRequest.TextInput text) {
            return new AiInvocationRequest.TextInput(text.text());
        }
        return new AiInvocationRequest.BlobInput(((EmbeddingBatchRequest.BlobInput) input).reference());
    }

    private static EmbeddingBatchResult batchResult(AiInvocationResult result) {
        return new EmbeddingBatchResult(
                result.outputs().stream().map(GovernedEmbeddingProvider::vector).toList(),
                result.providerRequestId());
    }

    private AiInvocationResult invoke(List<AiInvocationRequest.Input> inputs, EmbeddingPurpose purpose) {
        AiCapabilityDescriptor capability = AiInvocationRequests.requireCapability(
                capabilities(), AiCapabilityDescriptor.Operation.EMBEDDING);
        var request = AiInvocationRequests.createEmbedding(
                modelName(), modelVersion(), modelSignatureSha256(), capability,
                AiCapabilityDescriptor.ExecutionMode.BATCH, inputs, purpose);
        return service.invoke(request);
    }

    private static float[] vector(AiInvocationResult.Output output) {
        if (output instanceof AiInvocationResult.VectorOutput vector) {
            return vector.vector();
        }
        throw new IllegalStateException("Embedding invocation returned a non-vector output");
    }

    @Override public int dimensions() { return delegate.dimensions(); }
    @Override public Set<EmbeddingPurpose> supportedPurposes() { return delegate.supportedPurposes(); }
    @Override public String modelName() { return delegate.modelName(); }
    @Override public String modelVersion() { return delegate.modelVersion(); }
    @Override public byte[] modelSignatureSha256() {
        return delegate.modelSignatureSha256();
    }
    @Override public int maxBatchSize() { return delegate.maxBatchSize(); }
    @Override public Set<Modality> supportedModalities() { return delegate.supportedModalities(); }
    @Override public Set<AiCapabilityDescriptor> capabilities() { return delegate.capabilities(); }
}

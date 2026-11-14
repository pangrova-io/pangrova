package org.pangrova.ai.api.provider;

import org.pangrova.common.api.annotation.Experimental;

import java.net.URI;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import org.pangrova.ai.api.capability.AiCapabilityDescriptor.Operation;
import org.pangrova.ai.api.enums.Modality;
import org.pangrova.ai.api.embedding.BlobReference;
import org.pangrova.ai.api.model.AiModelDescriptor;
import org.pangrova.ai.api.embedding.EmbeddingProvider;
import org.pangrova.ai.api.inference.InferenceProvider;
import org.pangrova.ai.api.reranking.RerankingProvider;

/** Stateless dialect factory. Validation must not create clients, read secrets or perform network I/O.
 * Each successful create transfers one model-scoped provider to the Worker registry for closing. */
@Experimental
public interface AiProviderFactory {
    /** Inlined into provider binaries so discovery detects incompatible contracts. */
    int API_VERSION = 2;

    /** Contract version compiled into the provider. */
    int apiVersion();

    Set<String> dialects();

    void validate(String dialect, URI endpoint, Set<Operation> capabilities, Set<Modality> modalities,
                  int maxBatchSize, String modelName);

    default InferenceProvider createInference(AiModelDescriptor model, String credential,
            Optional<Function<BlobReference, byte[]>> blobLoader) {
        throw new UnsupportedOperationException("Inference unavailable for " + model.provider());
    }

    default EmbeddingProvider createEmbedding(AiModelDescriptor model, String credential) {
        throw new UnsupportedOperationException("Embedding unavailable for " + model.provider());
    }

    default RerankingProvider createReranking(AiModelDescriptor model, String credential) {
        throw new UnsupportedOperationException("Reranking unavailable for " + model.provider());
    }
}

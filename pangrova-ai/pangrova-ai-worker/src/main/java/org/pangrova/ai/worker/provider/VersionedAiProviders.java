package org.pangrova.ai.worker.provider;

import org.pangrova.ai.api.capability.AiCapabilityDescriptor;
import org.pangrova.ai.api.embedding.EmbeddingBatchRequest;
import org.pangrova.ai.api.embedding.EmbeddingPurpose;
import org.pangrova.ai.api.invocation.EmbeddingProvider;
import org.pangrova.ai.api.embedding.EmbeddingBatchResult;
import org.pangrova.ai.api.invocation.GenerationParameters;
import org.pangrova.ai.api.invocation.InferenceProvider;
import org.pangrova.ai.api.invocation.InferenceResult;
import org.pangrova.ai.api.capability.Modality;
import org.pangrova.ai.api.invocation.MultimodalPrompt;
import org.pangrova.ai.api.invocation.RerankingBatchResult;
import org.pangrova.ai.api.invocation.RerankingProvider;

import java.io.Closeable;
import java.util.List;
import java.util.Set;

/** Adds exact logical model identity to concrete provider implementations. */
final class VersionedAiProviders {

    private VersionedAiProviders() {}

    static InferenceProvider inference(
            InferenceProvider delegate,
            String modelName,
            String version,
            byte[] signature,
            Set<AiCapabilityDescriptor> capabilities) {
        return new VersionedInferenceProvider(
                delegate, modelName, version, signature, capabilities);
    }

    static EmbeddingProvider embedding(
            EmbeddingProvider delegate,
            String modelName,
            String version,
            byte[] signature,
            Set<AiCapabilityDescriptor> capabilities) {
        return new VersionedEmbeddingProvider(
                delegate, modelName, version, signature, capabilities);
    }

    static RerankingProvider reranking(
            RerankingProvider delegate,
            String modelName,
            String version,
            byte[] signature,
            Set<AiCapabilityDescriptor> capabilities) {
        return new VersionedRerankingProvider(
                delegate, modelName, version, signature, capabilities);
    }

    private static Set<Modality> supportedModalities(
            Set<AiCapabilityDescriptor> capabilities) {
        return capabilities.stream()
                .flatMap(capability -> capability.modalities().stream())
                .collect(java.util.stream.Collectors.toUnmodifiableSet());
    }

    private static void close(Object delegate) throws java.io.IOException {
        if (delegate instanceof Closeable closeable) {
            closeable.close();
        }
    }

    private static final class VersionedInferenceProvider
            implements InferenceProvider, Closeable {

        private final InferenceProvider delegate;
        private final String logicalModelName;
        private final String version;
        private final byte[] signature;
        private final Set<AiCapabilityDescriptor> capabilities;

        private VersionedInferenceProvider(
                InferenceProvider delegate,
                String logicalModelName,
                String version,
                byte[] signature,
                Set<AiCapabilityDescriptor> capabilities) {
            this.delegate = delegate;
            this.logicalModelName = logicalModelName;
            this.version = version;
            this.signature = signature.clone();
            this.capabilities = capabilities;
        }

        @Override
        public InferenceResult complete(String prompt, GenerationParameters parameters) {
            return delegate.complete(prompt, parameters);
        }

        @Override
        public List<InferenceResult> completeBatch(
                List<String> prompts, GenerationParameters parameters) {
            return delegate.completeBatch(prompts, parameters);
        }

        @Override
        public InferenceResult completeMultimodal(
                MultimodalPrompt prompt, GenerationParameters parameters) {
            return delegate.completeMultimodal(prompt, parameters);
        }

        @Override public String modelName() { return logicalModelName; }
        @Override public String modelVersion() { return version; }
        @Override public byte[] modelSignatureSha256() { return signature.clone(); }
        @Override public int maxBatchSize() { return delegate.maxBatchSize(); }
        @Override public Set<AiCapabilityDescriptor> capabilities() { return capabilities; }
        @Override public Set<Modality> supportedModalities() {
            return VersionedAiProviders.supportedModalities(capabilities);
        }
        @Override public void close() throws java.io.IOException {
            VersionedAiProviders.close(delegate);
        }
    }

    private static final class VersionedEmbeddingProvider
            implements EmbeddingProvider, Closeable {

        private final EmbeddingProvider delegate;
        private final String logicalModelName;
        private final String version;
        private final byte[] signature;
        private final Set<AiCapabilityDescriptor> capabilities;

        private VersionedEmbeddingProvider(
                EmbeddingProvider delegate,
                String logicalModelName,
                String version,
                byte[] signature,
                Set<AiCapabilityDescriptor> capabilities) {
            this.delegate = delegate;
            this.logicalModelName = logicalModelName;
            this.version = version;
            this.signature = signature.clone();
            this.capabilities = capabilities;
        }

        @Override public EmbeddingBatchResult embed(EmbeddingBatchRequest request) {
            return delegate.embed(request);
        }
        @Override public Set<EmbeddingPurpose> supportedPurposes() { return delegate.supportedPurposes(); }
        @Override public int dimensions() { return delegate.dimensions(); }
        @Override public String modelName() { return logicalModelName; }
        @Override public String modelVersion() { return version; }
        @Override public byte[] modelSignatureSha256() { return signature.clone(); }
        @Override public int maxBatchSize() { return delegate.maxBatchSize(); }
        @Override public Set<AiCapabilityDescriptor> capabilities() { return capabilities; }
        @Override public Set<Modality> supportedModalities() {
            return VersionedAiProviders.supportedModalities(capabilities);
        }
        @Override public void close() throws java.io.IOException {
            VersionedAiProviders.close(delegate);
        }
    }

    private static final class VersionedRerankingProvider
            implements RerankingProvider, Closeable {

        private final RerankingProvider delegate;
        private final String logicalModelName;
        private final String version;
        private final byte[] signature;
        private final Set<AiCapabilityDescriptor> capabilities;

        private VersionedRerankingProvider(
                RerankingProvider delegate,
                String logicalModelName,
                String version,
                byte[] signature,
                Set<AiCapabilityDescriptor> capabilities) {
            this.delegate = delegate;
            this.logicalModelName = logicalModelName;
            this.version = version;
            this.signature = signature.clone();
            this.capabilities = capabilities;
        }

        @Override public RerankingBatchResult score(
                String query, List<String> documents) {
            return delegate.score(query, documents);
        }
        @Override public String modelName() { return logicalModelName; }
        @Override public String modelVersion() { return version; }
        @Override public byte[] modelSignatureSha256() { return signature.clone(); }
        @Override public int maxBatchSize() { return delegate.maxBatchSize(); }
        @Override public Set<AiCapabilityDescriptor> capabilities() { return capabilities; }
        @Override public void close() throws java.io.IOException {
            VersionedAiProviders.close(delegate);
        }
    }
}

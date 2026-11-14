package org.pangrova.ai.provider.openai;

import org.pangrova.ai.http.AiModelProperties;

import org.pangrova.ai.provider.openai.embedding.OpenAiCompatibleEmbeddingProvider;
import org.pangrova.ai.provider.openai.embedding.OpenAiEmbeddingConfig;
import org.pangrova.ai.http.chat.OpenAiCompatibleInferenceProvider;
import org.pangrova.ai.http.chat.OpenAiInferenceConfig;
import org.pangrova.ai.provider.openai.inference.OpenAiResponsesInferenceProvider;
import org.pangrova.ai.provider.openai.inference.OpenAiTranscriptionInferenceProvider;
import org.pangrova.ai.provider.openai.rerank.OpenAiCompatibleRerankingProvider;
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
import org.pangrova.ai.api.provider.AiProviderFactory;

/** Creates clients for the three supported OpenAI wire dialects. */
public final class OpenAiProviderFactory implements AiProviderFactory {
    private static final String OPENAI_COMPATIBLE = "openai-compatible";
    private static final String OPENAI_RESPONSES = "openai-responses";
    private static final String OPENAI_TRANSCRIPTION = "openai-transcription";
    private static final int DEFAULT_TIMEOUT_SECONDS = 30;

    @Override
    public int apiVersion() {
        return API_VERSION;
    }

    @Override
    public Set<String> dialects() {
        return Set.of(OPENAI_COMPATIBLE, OPENAI_RESPONSES, OPENAI_TRANSCRIPTION);
    }

    @Override
    public void validate(String dialect, URI endpoint, Set<Operation> capabilities, Set<Modality> modalities,
                         int maxBatchSize, String modelName) {
        AiProviderDialectValidator.validate(dialect, endpoint, capabilities, modalities, maxBatchSize, modelName);
    }

    @Override
    public InferenceProvider createInference(AiModelDescriptor model, String apiKey,
            Optional<Function<BlobReference, byte[]>> blobLoader) {
        var config = OpenAiInferenceConfig.fromModel(model, apiKey);
        return switch (model.provider()) {
            case OPENAI_COMPATIBLE -> new OpenAiCompatibleInferenceProvider(
                    config, blobLoader);
            case OPENAI_RESPONSES -> new OpenAiResponsesInferenceProvider(
                    config, blobLoader);
            case OPENAI_TRANSCRIPTION -> new OpenAiTranscriptionInferenceProvider(
                    config, blobLoader.orElseThrow(() -> configurationFailure(
                            "OpenAI transcription model '" + model.name()
                                    + "' requires a governed FILE content loader")));
            default -> throw configurationFailure(
                    "Unsupported AI inference provider '" + model.provider() + "'");
        };
    }

    @Override
    public EmbeddingProvider createEmbedding(AiModelDescriptor model, String apiKey) {
        requireCompatibleProvider(model, "embedding");
        int dimension = AiModelProperties.integer(model, "embedding-dimension", -1);
        if (dimension <= 0) {
            throw configurationFailure(
                    "Model '" + model.name() + "' requires a positive embedding-dimension");
        }
        var config = new OpenAiEmbeddingConfig(
                model.endpoint().toString(),
                providerModel(model),
                apiKey,
                dimension,
                AiModelProperties.integer(model, "max-batch-size", 32),
                AiModelProperties.positiveDouble(
                        model, "max-requests-per-second", 10.0),
                model.properties().getOrDefault("query-prefix", ""),
                model.properties().getOrDefault("document-prefix", ""),
                AiModelProperties.integer(
                        model, "timeout-seconds", DEFAULT_TIMEOUT_SECONDS));
        return new OpenAiCompatibleEmbeddingProvider(config);
    }

    @Override
    public RerankingProvider createReranking(AiModelDescriptor model, String apiKey) {
        requireCompatibleProvider(model, "reranking");
        return new OpenAiCompatibleRerankingProvider(
                model.endpoint(),
                providerModel(model),
                apiKey,
                AiModelProperties.integer(model, "max-batch-size", 32),
                AiModelProperties.positiveDouble(
                        model, "max-requests-per-second", 10.0),
                AiModelProperties.integer(
                        model, "timeout-seconds", DEFAULT_TIMEOUT_SECONDS));
    }

    private static String providerModel(AiModelDescriptor model) {
        return model.properties().get("provider-model");
    }

    private static void requireCompatibleProvider(
            AiModelDescriptor model, String operation) {
        if (!OPENAI_COMPATIBLE.equals(model.provider())) {
            throw configurationFailure(
                    "AI " + operation + " requires provider '"
                            + OPENAI_COMPATIBLE + "'; model '" + model.name()
                            + "' configures '" + model.provider() + "'");
        }
    }

    private static IllegalArgumentException configurationFailure(String message) {
        return new IllegalArgumentException(message);
    }

}

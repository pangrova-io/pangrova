package org.pangrova.ai.qwen;

import java.net.URI;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import org.pangrova.ai.api.capability.AiCapabilityDescriptor.Operation;
import org.pangrova.ai.api.enums.Modality;
import org.pangrova.ai.api.embedding.BlobReference;
import org.pangrova.ai.api.model.AiModelDescriptor;
import org.pangrova.ai.http.chat.OpenAiInferenceConfig;
import org.pangrova.ai.http.chat.OpenAiCompatibleInferenceProvider;
import org.pangrova.ai.api.inference.InferenceProvider;
import org.pangrova.ai.api.provider.AiProviderFactory;

/** Alibaba Cloud Model Studio text generation using its OpenAI-compatible HTTP endpoint. */
public final class QwenProviderFactory implements AiProviderFactory {
    private static final String PATH = "/compatible-mode/v1/chat/completions";

    @Override
    public int apiVersion() {
        return API_VERSION;
    }

    @Override
    public Set<String> dialects() {
        return Set.of("qwen");
    }

    @Override
    public void validate(String dialect, URI endpoint, Set<Operation> capabilities, Set<Modality> modalities,
                         int maxBatchSize, String modelName) {
        if (!"qwen".equals(dialect) || !capabilities.equals(Set.of(Operation.TEXT_GENERATION))
                || !modalities.equals(Set.of(Modality.TEXT)) || !PATH.equals(endpoint.getRawPath())) {
            throw new IllegalArgumentException("Qwen model '" + modelName
                    + "' requires TEXT inference at " + PATH);
        }
    }

    @Override
    public InferenceProvider createInference(AiModelDescriptor model, String credential,
            Optional<Function<BlobReference, byte[]>> blobLoader) {
        return new OpenAiCompatibleInferenceProvider(
                OpenAiInferenceConfig.fromModel(model, credential), Optional.empty());
    }
}

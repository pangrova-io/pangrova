package org.pangrova.ai.provider.openai.inference;

import org.pangrova.ai.http.chat.OpenAiInferenceConfig;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.pangrova.ai.api.exception.AiInvocationException;
import org.pangrova.ai.api.enums.Modality;
import org.pangrova.ai.api.embedding.BlobReference;
import org.pangrova.ai.api.invocation.GenerationParameters;
import org.pangrova.ai.api.inference.InferenceProvider;
import org.pangrova.ai.api.invocation.InferenceResult;
import org.pangrova.ai.api.invocation.MultimodalPrompt;

import java.io.Closeable;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

/** Exact OpenAI audio transcription provider using multipart file upload. */
public final class OpenAiTranscriptionInferenceProvider
        implements InferenceProvider, Closeable {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final String modelName;
    private final OpenAiTranscriptionRequestEncoder encoder;
    private final OpenAiHttpRequester requester;

    /** Creates a transcription provider with mandatory governed FILE access. */
    public OpenAiTranscriptionInferenceProvider(
            OpenAiInferenceConfig config,
            Function<BlobReference, byte[]> blobLoader) {
        validate(config);
        this.modelName = config.modelName();
        this.encoder = new OpenAiTranscriptionRequestEncoder(modelName, blobLoader);
        this.requester = new OpenAiHttpRequester(config, "ai-transcription");
    }

    @Override
    public InferenceResult complete(String prompt, GenerationParameters parameters) {
        throw unsupported();
    }

    @Override
    public List<InferenceResult> completeBatch(
            List<String> prompts, GenerationParameters parameters) {
        throw unsupported();
    }

    @Override
    public InferenceResult completeMultimodal(
            MultimodalPrompt prompt, GenerationParameters parameters) {
        if (prompt == null) {
            throw new IllegalArgumentException("prompt must not be null");
        }
        GenerationParameters effective = parameters == null
                ? GenerationParameters.DEFAULTS : parameters;
        OpenAiTranscriptionRequestEncoder.EncodedRequest request = encoder.encode(
                prompt, effective);
        String response = requester.post(request.contentType(), request.body());
        return parseResponse(response);
    }

    @Override
    public String modelName() {
        return modelName;
    }

    @Override
    public int maxBatchSize() {
        return 1;
    }

    @Override
    public Set<Modality> supportedModalities() {
        return Set.of(Modality.TEXT, Modality.AUDIO);
    }

    @Override
    public void close() {
        requester.close();
    }

    private static InferenceResult parseResponse(String responseBody) {
        try {
            JsonNode root = MAPPER.readTree(responseBody);
            if (root == null || !root.isObject()) {
                throw invalidResponse("Transcription body must be a JSON object");
            }
            JsonNode text = root.get("text");
            if (text == null || !text.isTextual()) {
                throw invalidResponse("Transcription response has no text field");
            }
            return new InferenceResult(text.asText(), root.path("id").asText(""));
        } catch (JsonProcessingException failure) {
            throw invalidResponse("Transcription body is not valid JSON", failure);
        }
    }

    private static void validate(OpenAiInferenceConfig config) {
        java.util.Objects.requireNonNull(config, "config must not be null");
        validateIdentity(config);
        validateLimits(config);
    }

    private static void validateIdentity(OpenAiInferenceConfig config) {
        if (config.endpoint() == null || config.endpoint().isBlank()
                || config.modelName() == null || config.modelName().isBlank()
                || config.apiKey() == null) {
            throw new IllegalArgumentException(
                    "endpoint, modelName and apiKey must be configured");
        }
    }

    private static void validateLimits(OpenAiInferenceConfig config) {
        if (config.maxBatchSize() != 1 || config.maxRequestsPerSecond() <= 0
                || config.maxConcurrency() <= 0
                || config.timeoutSeconds() <= 0) {
            throw new IllegalArgumentException(
                    "transcription maxBatchSize must be 1 and provider limits must be positive");
        }
    }

    private static AiInvocationException unsupported() {
        return new AiInvocationException(
                AiInvocationException.Category.CAPABILITY_MISMATCH,
                false,
                "OpenAI transcription accepts only one audio FILE invocation");
    }

    private static AiInvocationException invalidResponse(String message) {
        return invalidResponse(message, null);
    }

    private static AiInvocationException invalidResponse(
            String message, Throwable cause) {
        return new AiInvocationException(
                AiInvocationException.Category.PROVIDER_RESPONSE_INVALID,
                false,
                message,
                cause);
    }
}

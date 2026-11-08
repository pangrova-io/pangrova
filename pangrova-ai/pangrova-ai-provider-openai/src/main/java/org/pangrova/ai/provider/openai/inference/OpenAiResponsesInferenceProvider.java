package org.pangrova.ai.provider.openai.inference;

import org.pangrova.ai.spi.invocation.AttemptAdmission;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.pangrova.common.concurrent.ManagedExecutorFactory;
import org.pangrova.ai.api.error.AiInvocationException;
import org.pangrova.ai.api.capability.Modality;
import org.pangrova.ai.api.embedding.BlobReference;
import org.pangrova.ai.api.invocation.GenerationParameters;
import org.pangrova.ai.spi.inference.InferenceProvider;
import org.pangrova.ai.api.invocation.InferenceResult;
import org.pangrova.ai.api.invocation.MultimodalPrompt;
import org.pangrova.ai.spi.invocation.AiProviderCallContext;

import java.io.Closeable;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

/** Exact OpenAI Responses API provider for text, image and document inputs. */
public final class OpenAiResponsesInferenceProvider
        implements InferenceProvider, Closeable {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final String modelName;
    private final int maxBatchSize;
    private final OpenAiResponsesRequestEncoder encoder;
    private final OpenAiHttpRequester requester;
    private final ExecutorService batchExecutor;
    private final boolean mediaEnabled;

    /** Creates a Responses API provider with optional governed FILE access. */
    public OpenAiResponsesInferenceProvider(
            OpenAiInferenceConfig config,
            Optional<Function<BlobReference, byte[]>> blobLoader) {
        validate(config);
        this.modelName = config.modelName();
        this.maxBatchSize = config.maxBatchSize();
        this.encoder = new OpenAiResponsesRequestEncoder(modelName, blobLoader);
        this.requester = new OpenAiHttpRequester(config, "ai-responses");
        this.batchExecutor = ManagedExecutorFactory.newFixedThreadPool(
                "ai-responses-batch",
                Math.min(config.maxBatchSize(), config.maxConcurrency()));
        this.mediaEnabled = blobLoader.isPresent();
    }

    @Override
    public InferenceResult complete(String prompt, GenerationParameters parameters) {
        if (prompt == null) {
            throw new IllegalArgumentException("prompt must not be null");
        }
        GenerationParameters effective = parameters == null
                ? GenerationParameters.DEFAULTS : parameters;
        return invoke(encoder.encodeText(prompt, effective));
    }

    @Override
    public List<InferenceResult> completeBatch(
            List<String> prompts, GenerationParameters parameters) {
        validateBatch(prompts);
        GenerationParameters effective = parameters == null
                ? GenerationParameters.DEFAULTS : parameters;
        String key = AiProviderCallContext.currentIdempotencyKey().orElse(null);
        Instant deadline = AiProviderCallContext.currentDeadline().orElse(null);
        AttemptAdmission attemptAdmission = AiProviderCallContext.currentAttemptAdmission()
                .orElse(null);
        return InferenceBatch.execute(prompts.size(), batchExecutor, index -> invokeBatchItem(
                prompts.get(index), effective, key, deadline, index, attemptAdmission));
    }

    @Override
    public InferenceResult completeMultimodal(
            MultimodalPrompt prompt, GenerationParameters parameters) {
        if (prompt == null) {
            throw new IllegalArgumentException("prompt must not be null");
        }
        GenerationParameters effective = parameters == null
                ? GenerationParameters.DEFAULTS : parameters;
        return invoke(encoder.encode(prompt, effective));
    }

    @Override
    public String modelName() {
        return modelName;
    }

    @Override
    public int maxBatchSize() {
        return maxBatchSize;
    }

    @Override
    public Set<Modality> supportedModalities() {
        return mediaEnabled
                ? Set.of(Modality.TEXT, Modality.IMAGE, Modality.DOCUMENT)
                : Set.of(Modality.TEXT);
    }

    @Override
    public void close() {
        requester.close();
        batchExecutor.shutdown();
    }

    private InferenceResult invokeBatchItem(
            String prompt,
            GenerationParameters parameters,
            String invocationKey,
            Instant deadline,
            int ordinal,
            AttemptAdmission attemptAdmission) {
        if (invocationKey == null) {
            return complete(prompt, parameters);
        }
        return AiProviderCallContext.callWithOrdinal(
                invocationKey, deadline, ordinal, attemptAdmission,
                () -> complete(prompt, parameters));
    }

    private InferenceResult invoke(String requestBody) {
        return parseResponse(requester.postJson(requestBody));
    }

    private static InferenceResult parseResponse(String responseBody) {
        try {
            JsonNode root = MAPPER.readTree(responseBody);
            if (root == null || !root.isObject()) {
                throw invalidResponse("Responses body must be a JSON object");
            }
            String text = outputText(root.path("output"));
            return new InferenceResult(text, root.path("id").asText(""));
        } catch (JsonProcessingException failure) {
            throw invalidResponse("Responses body is not valid JSON", failure);
        }
    }

    private static String outputText(JsonNode output) {
        if (!output.isArray()) {
            throw invalidResponse("Responses output must be an array");
        }
        StringBuilder text = new StringBuilder();
        for (JsonNode item : output) {
            JsonNode content = item.path("content");
            if (!content.isArray()) {
                continue;
            }
            for (JsonNode part : content) {
                if ("output_text".equals(part.path("type").asText())
                        && part.path("text").isTextual()) {
                    text.append(part.path("text").asText());
                }
            }
        }
        if (text.isEmpty()) {
            throw invalidResponse("Responses output has no output_text content");
        }
        return text.toString();
    }

    private void validateBatch(List<String> prompts) {
        if (prompts == null || prompts.isEmpty()
                || prompts.stream().anyMatch(java.util.Objects::isNull)) {
            throw new IllegalArgumentException(
                    "prompts must contain one or more non-null values");
        }
        if (prompts.size() > maxBatchSize) {
            throw new IllegalArgumentException(
                    "batch size " + prompts.size()
                            + " exceeds maxBatchSize " + maxBatchSize);
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
        if (config.maxBatchSize() <= 0 || config.maxRequestsPerSecond() <= 0
                || config.maxConcurrency() <= 0 || config.timeoutSeconds() <= 0) {
            throw new IllegalArgumentException("provider limits must be positive");
        }
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

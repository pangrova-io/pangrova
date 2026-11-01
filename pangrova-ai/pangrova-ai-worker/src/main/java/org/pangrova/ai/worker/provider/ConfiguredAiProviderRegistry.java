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
package org.pangrova.ai.worker.provider;

import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.spi.ai.capability.AiCapabilityDescriptor;
import org.pangrova.spi.ai.error.AiInvocationException;
import org.pangrova.spi.ai.model.AiModelConfigParser;
import org.pangrova.spi.ai.model.AiModelDescriptor;
import org.pangrova.spi.ai.embedding.BlobReference;
import org.pangrova.spi.ai.invocation.EmbeddingProvider;
import org.pangrova.spi.security.secret.SecretResolver;
import org.pangrova.spi.ai.invocation.InferenceProvider;
import org.pangrova.spi.ai.invocation.RerankingProvider;
import org.pangrova.ai.worker.embedding.provider.openai.OpenAiCompatibleEmbeddingProvider;
import org.pangrova.ai.worker.embedding.provider.openai.OpenAiEmbeddingConfig;
import org.pangrova.ai.worker.inference.provider.openai.OpenAiCompatibleInferenceProvider;
import org.pangrova.ai.worker.inference.provider.openai.OpenAiInferenceConfig;
import org.pangrova.ai.worker.inference.provider.openai.OpenAiResponsesInferenceProvider;
import org.pangrova.ai.worker.inference.provider.openai.OpenAiTranscriptionInferenceProvider;
import org.pangrova.ai.worker.rerank.provider.openai.OpenAiCompatibleRerankingProvider;

import java.io.Closeable;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
 * Worker-local, version-exact model registry built from the AI function plugin
 * configuration. Provider instances are created lazily and closed with the
 * worker function lifecycle.
 *
 * <p>Configuration keys use {@code models.<model-name>.<property>}. The model
 * name is the exact SQL-visible and provider-visible identifier; implicit
 * aliases are intentionally forbidden. Secret values are never accepted in
 * this map: {@code secret-reference} is resolved through {@link SecretResolver}
 * only when the first request for that model is executed.</p>
 */
@ThreadSafe
public final class ConfiguredAiProviderRegistry implements AutoCloseable {

    private static final String OPENAI_COMPATIBLE = "openai-compatible";
    private static final String OPENAI_RESPONSES = "openai-responses";
    private static final String OPENAI_TRANSCRIPTION = "openai-transcription";
    private static final int DEFAULT_TIMEOUT_SECONDS = 30;
    private static final int CLOSED_MASK = Integer.MIN_VALUE;
    private static final int ACTIVE_RESOLUTION_MASK = Integer.MAX_VALUE;

    private final Map<String, AiModelDescriptor> models;
    private final SecretResolver secretResolver;
    private final Optional<Function<BlobReference, byte[]>> blobLoader;
    private final ConcurrentMap<String, InferenceProvider> inferenceProviders =
            new ConcurrentHashMap<>();
    private final ConcurrentMap<String, EmbeddingProvider> embeddingProviders =
            new ConcurrentHashMap<>();
    private final ConcurrentMap<String, RerankingProvider> rerankingProviders =
            new ConcurrentHashMap<>();
    private final AtomicInteger lifecycle = new AtomicInteger();
    private final CompletableFuture<Void> resolutionsDrained = new CompletableFuture<>();
    private final CompletableFuture<Void> closeComplete = new CompletableFuture<>();

    public ConfiguredAiProviderRegistry(
            Map<String, String> config, SecretResolver secretResolver) {
        this(config, secretResolver, Optional.empty());
    }

    /** Creates a production registry with governed FILE content access. */
    public ConfiguredAiProviderRegistry(
            Map<String, String> config,
            SecretResolver secretResolver,
            Function<BlobReference, byte[]> blobLoader) {
        this(
                config,
                secretResolver,
                Optional.of(Objects.requireNonNull(blobLoader, "blobLoader required")));
    }

    private ConfiguredAiProviderRegistry(
            Map<String, String> config,
            SecretResolver secretResolver,
            Optional<Function<BlobReference, byte[]>> blobLoader) {
        this.secretResolver = Objects.requireNonNull(secretResolver, "secretResolver required");
        this.blobLoader = Objects.requireNonNull(blobLoader, "blobLoader must not be null");
        this.models = parseModels(
                config == null ? Map.of() : config,
                blobLoader.isPresent());
    }

    /** Returns immutable canonical descriptors for every enabled configured model. */
    public Collection<AiModelDescriptor> models() {
        return models.values();
    }

    /**
     * Parses enabled model descriptors without resolving credentials or creating
     * providers. Worker bootstrap uses this read-only view for exact model
     * capability advertisement.
     */
    public static Collection<AiModelDescriptor> describeModels(
            Map<String, String> config) {
        return describeModels(config, false);
    }

    /** Parses model inventory for a Worker whose governed FILE runtime initialized. */
    public static Collection<AiModelDescriptor> describeModels(
            Map<String, String> config,
            boolean governedBlobLoaderAvailable) {
        return parseModels(
                config == null ? Map.of() : config,
                governedBlobLoaderAvailable).values();
    }

    /** Resolves an inference provider by exact configured model name. */
    public InferenceProvider resolveInference(String modelName) {
        AiModelDescriptor model = requireModel(modelName);
        requireAnyCapability(
                model,
                AiCapabilityDescriptor.Operation.TEXT_GENERATION,
                AiCapabilityDescriptor.Operation.MULTIMODAL_GENERATION);
        return resolveProvider(
                modelName, model, inferenceProviders, this::createInference);
    }

    /** Resolves an embedding provider by exact configured model name. */
    public EmbeddingProvider resolveEmbedding(String modelName) {
        AiModelDescriptor model = requireModel(modelName);
        model.requireCapability(AiCapabilityDescriptor.Operation.EMBEDDING, model.version());
        return resolveProvider(
                modelName, model, embeddingProviders, this::createEmbedding);
    }

    /** Resolves a reranking provider by exact configured model name. */
    public RerankingProvider resolveReranking(String modelName) {
        AiModelDescriptor model = requireModel(modelName);
        model.requireCapability(
                AiCapabilityDescriptor.Operation.RERANKING, model.version());
        return resolveProvider(
                modelName, model, rerankingProviders, this::createReranking);
    }

    public boolean contains(String modelName) {
        return modelName != null && models.containsKey(modelName);
    }

    @Override
    public void close() {
        if (!beginClose()) {
            closeComplete.join();
            return;
        }
        RuntimeException failure = null;
        try {
            resolutionsDrained.join();
            failure = closeAll(inferenceProviders.values(), failure);
            failure = closeAll(embeddingProviders.values(), failure);
            failure = closeAll(rerankingProviders.values(), failure);
        } finally {
            inferenceProviders.clear();
            embeddingProviders.clear();
            rerankingProviders.clear();
            closeComplete.complete(null);
        }
        if (failure != null) {
            throw failure;
        }
    }

    private <T> T resolveProvider(
            String modelName,
            AiModelDescriptor model,
            ConcurrentMap<String, T> providers,
            Function<AiModelDescriptor, T> factory) {
        beginResolution();
        try {
            return providers.computeIfAbsent(modelName, ignored -> factory.apply(model));
        } finally {
            endResolution();
        }
    }

    private void beginResolution() {
        while (true) {
            int state = lifecycle.get();
            if ((state & CLOSED_MASK) != 0) {
                throw new IllegalStateException("AI provider registry has been closed");
            }
            if ((state & ACTIVE_RESOLUTION_MASK) == ACTIVE_RESOLUTION_MASK) {
                throw new IllegalStateException("Too many concurrent AI provider resolutions");
            }
            if (lifecycle.compareAndSet(state, state + 1)) {
                return;
            }
        }
    }

    private void endResolution() {
        int state = lifecycle.decrementAndGet();
        if (state == CLOSED_MASK) {
            resolutionsDrained.complete(null);
        }
    }

    private boolean beginClose() {
        while (true) {
            int state = lifecycle.get();
            if ((state & CLOSED_MASK) != 0) {
                return false;
            }
            if (lifecycle.compareAndSet(state, state | CLOSED_MASK)) {
                if (state == 0) {
                    resolutionsDrained.complete(null);
                }
                return true;
            }
        }
    }

    private InferenceProvider createInference(AiModelDescriptor model) {
        requireSupportedProvider(model);
        String apiKey = resolveSecret(model);
        int maxBatchSize = AiModelProperties.integer(model, "max-batch-size", 16);
        double maxRequestsPerSecond = AiModelProperties.positiveDouble(
                model, "max-requests-per-second", 10.0);
        int timeoutSeconds = AiModelProperties.integer(
                model, "timeout-seconds", DEFAULT_TIMEOUT_SECONDS);
        OpenAiInferenceConfig config = new OpenAiInferenceConfig(
                model.endpoint().toString(), providerModel(model), apiKey,
                maxBatchSize, maxRequestsPerSecond,
                AiModelProperties.integer(model, "max-concurrency", -1),
                timeoutSeconds);
        InferenceProvider delegate = switch (model.provider()) {
            case OPENAI_COMPATIBLE -> new OpenAiCompatibleInferenceProvider(
                    config, blobLoaderFor(model));
            case OPENAI_RESPONSES -> new OpenAiResponsesInferenceProvider(
                    config, blobLoaderFor(model));
            case OPENAI_TRANSCRIPTION -> new OpenAiTranscriptionInferenceProvider(
                    config, blobLoaderFor(model).orElseThrow(() -> configurationFailure(
                            "OpenAI transcription model '" + model.name()
                                    + "' requires a governed FILE content loader")));
            default -> throw configurationFailure(
                    "Unsupported AI inference provider '" + model.provider() + "'");
        };
        Set<AiCapabilityDescriptor> capabilities = capabilitiesFor(
                model, AiCapabilityDescriptor.Operation.TEXT_GENERATION,
                AiCapabilityDescriptor.Operation.MULTIMODAL_GENERATION);
        return VersionedAiProviders.inference(
                delegate,
                model.name(),
                model.version(),
                org.pangrova.spi.ai.model.AiModelIdentity.signatureSha256(model),
                capabilities);
    }

    private EmbeddingProvider createEmbedding(AiModelDescriptor model) {
        requireCompatibleProvider(model, "embedding");
        int dimension = AiModelProperties.integer(model, "embedding-dimension", -1);
        if (dimension <= 0) {
            throw configurationFailure(
                    "Model '" + model.name() + "' requires a positive embedding-dimension");
        }
        String apiKey = resolveSecret(model);
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
        EmbeddingProvider delegate = new OpenAiCompatibleEmbeddingProvider(config);
        Set<AiCapabilityDescriptor> capabilities = capabilitiesFor(
                model, AiCapabilityDescriptor.Operation.EMBEDDING);
        return VersionedAiProviders.embedding(
                delegate,
                model.name(),
                model.version(),
                org.pangrova.spi.ai.model.AiModelIdentity.signatureSha256(model),
                capabilities);
    }

    private RerankingProvider createReranking(AiModelDescriptor model) {
        requireCompatibleProvider(model, "reranking");
        RerankingProvider delegate = new OpenAiCompatibleRerankingProvider(
                model.endpoint(),
                providerModel(model),
                resolveSecret(model),
                AiModelProperties.integer(model, "max-batch-size", 32),
                AiModelProperties.positiveDouble(
                        model, "max-requests-per-second", 10.0),
                AiModelProperties.integer(
                        model, "timeout-seconds", DEFAULT_TIMEOUT_SECONDS));
        return VersionedAiProviders.reranking(
                delegate,
                model.name(),
                model.version(),
                org.pangrova.spi.ai.model.AiModelIdentity.signatureSha256(model),
                capabilitiesFor(
                        model, AiCapabilityDescriptor.Operation.RERANKING));
    }

    private String resolveSecret(AiModelDescriptor model) {
        if (model.properties().getOrDefault("authentication", "bearer")
                .equals("none")) {
            return "";
        }
        final String secret;
        try {
            secret = secretResolver.resolve(model.secretReference());
        } catch (RuntimeException failure) {
            throw new AiInvocationException(
                    AiInvocationException.Category.AUTHENTICATION,
                    false,
                    "Credential resolution failed for AI model '"
                            + model.name() + "'",
                    failure);
        }
        if (secret == null || secret.isBlank()) {
            throw new AiInvocationException(
                    AiInvocationException.Category.AUTHENTICATION,
                    false,
                    "Credential resolution returned no value for AI model '"
                            + model.name() + "'");
        }
        return secret;
    }

    private static String providerModel(AiModelDescriptor model) {
        return model.properties().get("provider-model");
    }

    private Optional<Function<BlobReference, byte[]>> blobLoaderFor(
            AiModelDescriptor model) {
        boolean multimodal = model.capabilities().stream().anyMatch(capability ->
                capability.operation()
                        == AiCapabilityDescriptor.Operation.MULTIMODAL_GENERATION);
        if (!multimodal) {
            return Optional.empty();
        }
        Function<BlobReference, byte[]> loader = blobLoader.orElseThrow(() ->
                configurationFailure(
                        "AI model '" + model.name() + "' cannot execute multimodal: "
                                + "no governed FILE content loader is installed"));
        return Optional.of(loader);
    }

    private AiModelDescriptor requireModel(String modelName) {
        if (modelName == null || modelName.isBlank()) {
            throw new AiInvocationException(
                    AiInvocationException.Category.MODEL_NOT_FOUND,
                    false,
                    "AI model name must not be blank");
        }
        AiModelDescriptor model = models.get(modelName);
        if (model == null) {
            throw new AiInvocationException(
                    AiInvocationException.Category.MODEL_NOT_FOUND,
                    false,
                    "AI model '" + modelName + "' is not configured; available models="
                            + models.keySet());
        }
        return model;
    }

    private static void requireAnyCapability(
            AiModelDescriptor model, AiCapabilityDescriptor.Operation... operations) {
        for (AiCapabilityDescriptor.Operation operation : operations) {
            boolean present = model.capabilities().stream()
                    .anyMatch(capability -> capability.operation() == operation);
            if (present) {
                return;
            }
        }
        throw new AiInvocationException(
                AiInvocationException.Category.CAPABILITY_MISMATCH,
                false,
                "Model '" + model.name() + "' has no inference capability");
    }

    private static Map<String, AiModelDescriptor> parseModels(
            Map<String, String> config,
            boolean governedBlobLoaderAvailable) {
        return AiModelConfigParser.parse(config, governedBlobLoaderAvailable);
    }

    private static void requireSupportedProvider(AiModelDescriptor model) {
        if (!Set.of(
                OPENAI_COMPATIBLE,
                OPENAI_RESPONSES,
                OPENAI_TRANSCRIPTION).contains(model.provider())) {
            throw configurationFailure(
                    "Unsupported AI provider '" + model.provider() + "' for model '"
                            + model.name() + "'");
        }
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

    private static Set<AiCapabilityDescriptor> capabilitiesFor(
            AiModelDescriptor model, AiCapabilityDescriptor.Operation... operations) {
        Set<AiCapabilityDescriptor.Operation> selected = Set.of(operations);
        return model.capabilities().stream()
                .filter(capability -> selected.contains(capability.operation()))
                .collect(java.util.stream.Collectors.toUnmodifiableSet());
    }

    private static RuntimeException closeAll(
            Collection<?> providers, RuntimeException firstFailure) {
        for (Object provider : providers) {
            if (!(provider instanceof Closeable closeable)) {
                continue;
            }
            try {
                closeable.close();
            } catch (Exception failure) {
                if (firstFailure == null) {
                    firstFailure = new IllegalStateException("Failed to close AI provider", failure);
                } else {
                    firstFailure.addSuppressed(failure);
                }
            }
        }
        return firstFailure;
    }

    private static IllegalArgumentException configurationFailure(String message) {
        return new IllegalArgumentException(message);
    }

}

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

import org.pangrova.ai.api.enums.AiFailureCategory;
import org.pangrova.ai.api.enums.AiOperation;
import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.ai.api.capability.AiCapabilityDescriptor;
import org.pangrova.ai.api.exception.AiInvocationException;
import org.pangrova.ai.control.config.AiModelConfigParser;
import org.pangrova.ai.control.config.AiProviderFactories;
import org.pangrova.ai.api.model.AiModelDescriptor;
import org.pangrova.ai.api.embedding.BlobReference;
import org.pangrova.ai.api.embedding.EmbeddingProvider;
import org.pangrova.security.api.secret.SecretResolver;
import org.pangrova.ai.api.inference.InferenceProvider;
import org.pangrova.ai.api.reranking.RerankingProvider;

import org.pangrova.ai.worker.provider.VersionedAiProviders.VersionedInferenceProvider;
import org.pangrova.ai.worker.provider.VersionedAiProviders.VersionedEmbeddingProvider;
import org.pangrova.ai.worker.provider.VersionedAiProviders.VersionedRerankingProvider;
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

    private static final int CLOSED_MASK = Integer.MIN_VALUE;
    private static final int ACTIVE_RESOLUTION_MASK = Integer.MAX_VALUE;

    private final Map<String, AiModelDescriptor> models;
    private final AiProviderFactories factories;
    private final SecretResolver secretResolver;
    private final Optional<Function<BlobReference, byte[]>> blobLoader;
    private final ConcurrentMap<String, VersionedInferenceProvider> inferenceProviders =
            new ConcurrentHashMap<>();
    private final ConcurrentMap<String, VersionedEmbeddingProvider> embeddingProviders =
            new ConcurrentHashMap<>();
    private final ConcurrentMap<String, VersionedRerankingProvider> rerankingProviders =
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
        this.factories = AiProviderFactories.discover();
        this.models = AiModelConfigParser.parse(
                config == null ? Map.of() : config,
                blobLoader.isPresent(), factories);
    }

    /** Returns immutable canonical descriptors for every enabled configured model. */
    public Collection<AiModelDescriptor> models() {
        return models.values();
    }

    /** Resolves an inference provider by exact configured model name. */
    public InferenceProvider resolveInference(String modelName) {
        AiModelDescriptor model = requireModel(modelName);
        requireAnyCapability(
                model,
                AiOperation.TEXT_GENERATION,
                AiOperation.MULTIMODAL_GENERATION);
        return resolveProvider(
                modelName, model, inferenceProviders, this::createInference);
    }

    /** Resolves an embedding provider by exact configured model name. */
    public EmbeddingProvider resolveEmbedding(String modelName) {
        AiModelDescriptor model = requireModel(modelName);
        model.requireCapability(AiOperation.EMBEDDING, model.version());
        return resolveProvider(
                modelName, model, embeddingProviders, this::createEmbedding);
    }

    /** Resolves a reranking provider by exact configured model name. */
    public RerankingProvider resolveReranking(String modelName) {
        AiModelDescriptor model = requireModel(modelName);
        model.requireCapability(
                AiOperation.RERANKING, model.version());
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
        try {
            resolutionsDrained.join();
            var releases = java.util.stream.Stream.<Collection<? extends Closeable>>of(
                            inferenceProviders.values(), embeddingProviders.values(),
                            rerankingProviders.values())
                    .flatMap(Collection::stream)
                    .<Runnable>map(provider -> () -> closeProvider(provider))
                    .toArray(Runnable[]::new);
            org.pangrova.common.lifecycle.ResourceCloser.closeAll(null, releases);
        } catch (RuntimeException | Error failure) {
            closeComplete.completeExceptionally(failure);
            throw failure;
        } finally {
            inferenceProviders.clear();
            embeddingProviders.clear();
            rerankingProviders.clear();
        }
        closeComplete.complete(null);
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

    private VersionedInferenceProvider createInference(AiModelDescriptor model) {
        InferenceProvider delegate = factories.require(model.provider(), model.name())
                .createInference(model, resolveSecret(model), blobLoaderFor(model));
        Set<AiCapabilityDescriptor> capabilities = capabilitiesFor(
                model, AiOperation.TEXT_GENERATION,
                AiOperation.MULTIMODAL_GENERATION);
        return VersionedAiProviders.inference(
                delegate,
                model.name(),
                model.version(),
                org.pangrova.ai.api.model.AiModelIdentity.signatureSha256(model),
                capabilities);
    }

    private VersionedEmbeddingProvider createEmbedding(AiModelDescriptor model) {
        EmbeddingProvider delegate = factories.require(model.provider(), model.name())
                .createEmbedding(model, resolveSecret(model));
        Set<AiCapabilityDescriptor> capabilities = capabilitiesFor(
                model, AiOperation.EMBEDDING);
        return VersionedAiProviders.embedding(
                delegate,
                model.name(),
                model.version(),
                org.pangrova.ai.api.model.AiModelIdentity.signatureSha256(model),
                capabilities);
    }

    private VersionedRerankingProvider createReranking(AiModelDescriptor model) {
        RerankingProvider delegate = factories.require(model.provider(), model.name())
                .createReranking(model, resolveSecret(model));
        return VersionedAiProviders.reranking(
                delegate,
                model.name(),
                model.version(),
                org.pangrova.ai.api.model.AiModelIdentity.signatureSha256(model),
                capabilitiesFor(
                        model, AiOperation.RERANKING));
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
                    AiFailureCategory.AUTHENTICATION,
                    false,
                    "Credential resolution failed for AI model '"
                            + model.name() + "'",
                    failure);
        }
        if (secret == null || secret.isBlank()) {
            throw new AiInvocationException(
                    AiFailureCategory.AUTHENTICATION,
                    false,
                    "Credential resolution returned no value for AI model '"
                            + model.name() + "'");
        }
        return secret;
    }

    private Optional<Function<BlobReference, byte[]>> blobLoaderFor(
            AiModelDescriptor model) {
        boolean multimodal = model.capabilities().stream().anyMatch(capability ->
                capability.operation()
                        == AiOperation.MULTIMODAL_GENERATION);
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
                    AiFailureCategory.MODEL_NOT_FOUND,
                    false,
                    "AI model name must not be blank");
        }
        AiModelDescriptor model = models.get(modelName);
        if (model == null) {
            throw new AiInvocationException(
                    AiFailureCategory.MODEL_NOT_FOUND,
                    false,
                    "AI model '" + modelName + "' is not configured; available models="
                            + models.keySet());
        }
        return model;
    }

    private static void requireAnyCapability(
            AiModelDescriptor model, AiOperation... operations) {
        for (AiOperation operation : operations) {
            boolean present = model.capabilities().stream()
                    .anyMatch(capability -> capability.operation() == operation);
            if (present) {
                return;
            }
        }
        throw new AiInvocationException(
                AiFailureCategory.CAPABILITY_MISMATCH,
                false,
                "Model '" + model.name() + "' has no inference capability");
    }

    private static Set<AiCapabilityDescriptor> capabilitiesFor(
            AiModelDescriptor model, AiOperation... operations) {
        Set<AiOperation> selected = Set.of(operations);
        return model.capabilities().stream()
                .filter(capability -> selected.contains(capability.operation()))
                .collect(java.util.stream.Collectors.toUnmodifiableSet());
    }

    private static void closeProvider(Closeable provider) {
        try {
            provider.close();
        } catch (java.io.IOException failure) {
            throw new IllegalStateException("Failed to close AI provider", failure);
        }
    }

    private static IllegalArgumentException configurationFailure(String message) {
        return new IllegalArgumentException(message);
    }

}

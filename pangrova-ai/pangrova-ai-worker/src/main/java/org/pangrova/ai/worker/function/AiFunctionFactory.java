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
package org.pangrova.ai.worker.function;

import org.pangrova.ai.worker.config.AiFunctionActivation;
import org.pangrova.ai.api.embedding.EmbeddingProvider;
import org.pangrova.ai.api.inference.InferenceProvider;
import org.pangrova.ai.api.reranking.RerankingProvider;
import org.pangrova.function.api.registry.FunctionFactory;
import org.pangrova.function.api.scalar.ScalarFunction;
import org.pangrova.ai.worker.governance.AiInvocationAuthorizer;
import org.pangrova.ai.worker.governance.observer.AiInvocationObserver;
import org.pangrova.ai.worker.governance.observer.CompositeAiInvocationObserver;
import org.pangrova.ai.worker.invocation.AiInvocationService;
import org.pangrova.ai.worker.governance.ConfigurableAiInvocationAuthorizer;
import org.pangrova.ai.worker.provider.ConfiguredAiProviderRegistry;
import org.pangrova.security.api.secret.SecretResolver;
import org.pangrova.security.secret.resolution.SecretResolverRegistry;
import org.pangrova.ai.worker.provider.governed.GovernedEmbeddingProvider;
import org.pangrova.ai.worker.provider.governed.GovernedInferenceProvider;
import org.pangrova.ai.worker.provider.governed.GovernedRerankingProvider;
import org.pangrova.ai.worker.blob.GovernedBlobStore;
import org.pangrova.ai.worker.blob.HttpsBlobImporter;
import org.pangrova.filesystem.api.blob.BlobStorage;
import org.pangrova.filesystem.api.blob.BlobStorageFactory;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.function.Function;

/**
 * FunctionFactory that registers all generative AI SQL functions.
 *
 * <p>Provides: ai_complete, ai_summarize, ai_classify, ai_filter, ai_embed,
 * ai_extract, ai_guard, ai_predict, ai_translate. Most functions use the
 * {@link InferenceProvider} SPI; {@code ai_embed} uses the exact configured
 * {@link EmbeddingProvider} deployment.</p>
 *
 * <p>No-arg constructor for {@link ServiceLoader} discovery. Each provider is
 * resolved lazily from the Worker model catalog; functions fail before provider
 * I/O when no exact deployment is configured.</p>
 */
public final class AiFunctionFactory implements FunctionFactory, AutoCloseable {
    private static final Duration FILE_STAGING_CLEANUP_AGE = Duration.ofHours(24);
    private static final int FILE_STAGING_CLEANUP_LIMIT = 10_000;
    private static final long DEFAULT_CONNECTION_TTL_MILLIS = 60_000;
    private static final long DEFAULT_IDLE_EVICTION_MILLIS = 30_000;
    private static final long DEFAULT_CONNECT_TIMEOUT_MILLIS = 5_000;
    private static final long DEFAULT_READ_TIMEOUT_MILLIS = 30_000;

    private final Function<String, InferenceProvider> providerLookup;
    private final Function<String, EmbeddingProvider> embeddingLookup;
    private final Function<String, RerankingProvider> rerankingLookup;
    private final BlobRuntime injectedBlobRuntime;
    private final boolean productionGovernance;
    private ProductionRuntime productionRuntime;
    private boolean closed;

    public AiFunctionFactory() {
        this(null, null, null, null, true);
    }

    AiFunctionFactory(
            Function<String, InferenceProvider> providerLookup,
            Function<String, EmbeddingProvider> embeddingLookup,
            Function<String, RerankingProvider> rerankingLookup,
            GovernedBlobStore blobStore,
            HttpsBlobImporter blobImporter) {
        this(
                providerLookup, embeddingLookup, rerankingLookup,
                new BlobRuntime(blobStore, blobImporter), false);
    }

    private AiFunctionFactory(
            Function<String, InferenceProvider> providerLookup,
            Function<String, EmbeddingProvider> embeddingLookup,
            Function<String, RerankingProvider> rerankingLookup,
            BlobRuntime injectedBlobRuntime,
            boolean productionGovernance) {
        if (!productionGovernance) {
            requireInjectedLookups(
                    providerLookup, embeddingLookup, rerankingLookup,
                    injectedBlobRuntime);
        }
        this.providerLookup = providerLookup;
        this.embeddingLookup = embeddingLookup;
        this.rerankingLookup = rerankingLookup;
        this.injectedBlobRuntime = injectedBlobRuntime;
        this.productionGovernance = productionGovernance;
    }

    private static void requireInjectedLookups(
            Function<String, InferenceProvider> providerLookup,
            Function<String, EmbeddingProvider> embeddingLookup,
            Function<String, RerankingProvider> rerankingLookup,
            BlobRuntime blobRuntime) {
        if (providerLookup == null) {
            throw new IllegalArgumentException("providerLookup required");
        }
        if (embeddingLookup == null) {
            throw new IllegalArgumentException("embeddingLookup required");
        }
        if (rerankingLookup == null) {
            throw new IllegalArgumentException("rerankingLookup required");
        }
        if (blobRuntime == null) {
            throw new IllegalArgumentException("blob runtime required");
        }
    }

    @Override
    public String getName() {
        return "ai-inference";
    }

    @Override
    public synchronized List<ScalarFunction> getScalarFunctions(Map<String, String> config) {
        if (closed) {
            throw new IllegalStateException("AI function factory is closed");
        }
        if (productionGovernance) {
            return productionFunctions(config == null ? Map.of() : config);
        }
        return functions(
                new GovernedLookups(providerLookup, embeddingLookup, rerankingLookup),
                injectedBlobRuntime);
    }

    private List<ScalarFunction> productionFunctions(
            Map<String, String> config) {
        if (productionRuntime == null) {
            productionRuntime = createProductionRuntime(config);
        }
        return enabledFunctions(productionRuntime);
    }

    private static List<ScalarFunction> enabledFunctions(ProductionRuntime runtime) {
        return functions(runtime.lookups(), runtime.blobs());
    }

    private static List<ScalarFunction> functions(
            GovernedLookups lookups, BlobRuntime blobs) {
        List<ScalarFunction> ai = lookups == null ? List.of()
                : AiFunctionCatalog.aiFunctions(
                        lookups.inference(), lookups.embedding(), lookups.reranking());
        List<ScalarFunction> file = blobs == null ? List.of()
                : AiFunctionCatalog.fileFunctions(blobs.store(), blobs.importer());
        return AiFunctionCatalog.combine(ai, file);
    }

    private ProductionRuntime createProductionRuntime(Map<String, String> config) {
        boolean modelsEnabled = AiFunctionActivation.hasModels(config);
        boolean fileEnabled = AiFunctionActivation.hasFileStorage(config);
        if (!modelsEnabled && !fileEnabled) {
            return ProductionRuntime.disabled();
        }
        SecretResolver secretResolver = SecretResolverRegistry.discover(AiFunctionFactory.class.getClassLoader());
        BlobRuntime blobs = createBlobRuntimeIfEnabled(
                config, secretResolver, fileEnabled);
        ConfiguredAiProviderRegistry configured = null;
        try {
            configured = createProviderRegistry(
                    config, secretResolver, blobs, modelsEnabled);
            GovernedLookups lookups = configured == null
                    ? null
                    : createGovernedLookups(config, configured);
            List<AutoCloseable> resources = ownedResources(configured, blobs);
            return new ProductionRuntime(lookups, blobs, resources);
        } catch (RuntimeException | Error failure) {
            AiFunctionCatalog.closeAfterFailure(failure, configured, blobs);
            throw failure;
        }
    }

    private static BlobRuntime createBlobRuntimeIfEnabled(
            Map<String, String> config,
            SecretResolver secretResolver,
            boolean enabled) {
        return enabled ? createBlobRuntime(config, secretResolver) : null;
    }

    private static ConfiguredAiProviderRegistry createProviderRegistry(
            Map<String, String> config,
            SecretResolver secretResolver,
            BlobRuntime blobs,
            boolean enabled) {
        if (!enabled) {
            return null;
        }
        return blobs == null
                ? new ConfiguredAiProviderRegistry(config, secretResolver)
                : new ConfiguredAiProviderRegistry(
                        config, secretResolver, blobs.store()::read);
    }

    private static List<AutoCloseable> ownedResources(
            ConfiguredAiProviderRegistry configured, BlobRuntime blobs) {
        if (configured == null) {
            return List.of(blobs);
        }
        return blobs == null ? List.of(configured) : List.of(configured, blobs);
    }

    private GovernedLookups createGovernedLookups(
            Map<String, String> config, ConfiguredAiProviderRegistry configured) {
        Function<String, InferenceProvider> inference = configured::resolveInference;
        Function<String, EmbeddingProvider> embedding = configured::resolveEmbedding;
        Function<String, RerankingProvider> reranking = configured::resolveReranking;
        return governedLookups(config, inference, embedding, reranking);
    }

    private GovernedLookups governedLookups(
            Map<String, String> config,
            Function<String, InferenceProvider> inferenceLookup,
            Function<String, EmbeddingProvider> configuredEmbeddingLookup,
            Function<String, RerankingProvider> configuredRerankingLookup) {
        AiInvocationAuthorizer authorizer = ConfigurableAiInvocationAuthorizer.discover(config);
        AiInvocationObserver observer = CompositeAiInvocationObserver.discover();
        AiInvocationService service = new AiInvocationService(
                inferenceLookup,
                configuredEmbeddingLookup,
                configuredRerankingLookup,
                authorizer,
                observer);
        return new GovernedLookups(
                modelName -> new GovernedInferenceProvider(
                        requireExactModel(modelName, inferenceLookup.apply(modelName)), service),
                modelName -> new GovernedEmbeddingProvider(
                        requireExactModel(
                                modelName,
                                configuredEmbeddingLookup.apply(modelName)),
                        service),
                modelName -> new GovernedRerankingProvider(
                        requireExactModel(
                                modelName,
                                configuredRerankingLookup.apply(modelName)),
                        service));
    }

    private static <T> T requireExactModel(String requestedName, T provider) {
        if (provider == null) {
            throw new IllegalStateException("No provider registered for model '" + requestedName + "'");
        }
        String actualName;
        if (provider instanceof InferenceProvider inference) {
            actualName = inference.modelName();
        } else if (provider instanceof EmbeddingProvider embedding) {
            actualName = embedding.modelName();
        } else if (provider instanceof RerankingProvider reranking) {
            actualName = reranking.modelName();
        } else {
            throw new IllegalArgumentException("Unsupported AI provider type: " + provider.getClass());
        }
        if (!requestedName.equals(actualName)) {
            throw new IllegalStateException(
                    "Provider model mismatch: requested '" + requestedName
                            + "' but provider serves '" + actualName + "'");
        }
        return provider;
    }

    @Override
    public synchronized void close() throws Exception {
        if (closed) {
            return;
        }
        closed = true;
        var runtime = productionRuntime;
        productionRuntime = null;
        if (runtime == null) {
            return;
        }
        Throwable firstFailure = null;
        for (var resource : runtime.resources()) {
            try {
                resource.close();
            } catch (Exception | Error failure) {
                if (firstFailure == null) {
                    firstFailure = failure;
                } else if (firstFailure != failure) {
                    firstFailure.addSuppressed(failure);
                }
            }
        }
        if (firstFailure instanceof Exception exception) {
            throw exception;
        }
        if (firstFailure instanceof Error error) {
            throw error;
        }
    }

    private record GovernedLookups(Function<String, InferenceProvider> inference,
            Function<String, EmbeddingProvider> embedding,
            Function<String, RerankingProvider> reranking) {}

    private record ProductionRuntime(
            GovernedLookups lookups,
            BlobRuntime blobs,
            List<AutoCloseable> resources) {
        private static ProductionRuntime disabled() {
            return new ProductionRuntime(null, null, List.of());
        }
    }

    private static BlobRuntime createBlobRuntime(
            Map<String, String> config,
            SecretResolver secretResolver) {
        Set<String> mimeTypes = csv(config.getOrDefault(
                "file.allowed-mime-types",
                "image/jpeg,image/png,image/webp,application/pdf,text/plain,"
                        + "audio/mpeg,audio/wav,video/mp4"));
        long maxBytes = positiveLong(config, "file.max-bytes", 64L * 1024 * 1024);
        byte[] capabilityKey = org.pangrova.ai.worker.blob.TenantCapabilitySigner
                .decodeBase64Secret(secretResolver.resolve(required(
                        config, "file.capability-secret-reference")));
        try {
            return createBlobRuntime(config, mimeTypes, maxBytes, capabilityKey);
        } finally {
            Arrays.fill(capabilityKey, (byte) 0);
        }
    }

    private static BlobRuntime createBlobRuntime(
            Map<String, String> config,
            Set<String> mimeTypes,
            long maxBytes,
            byte[] capabilityKey) {
        BlobStorage storage = createStorage(config, ServiceLoader.load(BlobStorageFactory.class,
                AiFunctionFactory.class.getClassLoader()));
        GovernedBlobStore store;
        try {
            store = new GovernedBlobStore(storage,
                    Path.of(required(config, "file.storage-staging-root")),
                    capabilityKey, maxBytes, mimeTypes);
        } catch (RuntimeException | Error failure) {
            AiFunctionCatalog.closeAfterFailure(failure, storage);
            throw failure;
        }
        try {
            HttpsBlobImporter importer = createImporter(
                    config, store, mimeTypes, maxBytes);
            store.cleanupStaging(
                    FILE_STAGING_CLEANUP_AGE, FILE_STAGING_CLEANUP_LIMIT);
            return new BlobRuntime(store, importer);
        } catch (RuntimeException | Error failure) {
            AiFunctionCatalog.closeAfterFailure(failure, store);
            throw failure;
        }
    }

    private static HttpsBlobImporter createImporter(
            Map<String, String> config,
            GovernedBlobStore store,
            Set<String> mimeTypes,
            long maxBytes) {
        HttpsBlobImporter.PoolSettings pool = new HttpsBlobImporter.PoolSettings(
                positiveInt(config, "file.url.max-connections", 256),
                positiveInt(config, "file.url.max-connections-per-route", 32),
                Duration.ofMillis(positiveLong(
                        config, "file.url.connection-ttl-ms",
                        DEFAULT_CONNECTION_TTL_MILLIS)),
                Duration.ofMillis(positiveLong(
                        config, "file.url.idle-eviction-ms",
                        DEFAULT_IDLE_EVICTION_MILLIS)));
        return new HttpsBlobImporter(store, new HttpsBlobImporter.Settings(
                csv(config.getOrDefault("file.url.allowed-hosts", "")), mimeTypes,
                Duration.ofMillis(positiveLong(
                        config, "file.url.connect-timeout-ms",
                        DEFAULT_CONNECT_TIMEOUT_MILLIS)),
                Duration.ofMillis(positiveLong(
                        config, "file.url.read-timeout-ms",
                        DEFAULT_READ_TIMEOUT_MILLIS)),
                positiveLong(config, "file.url.max-wire-bytes", maxBytes),
                positiveLong(config, "file.url.max-decoded-bytes", maxBytes), pool));
    }

    static BlobStorage createStorage(Map<String, String> config, Iterable<BlobStorageFactory> factories) {
        var factory = BlobStorageFactory.select(config, factories);
        return java.util.Objects.requireNonNull(factory.create(config), "FILE storage factory returned null");
    }

    private static String required(Map<String, String> config, String key) {
        String value = config.get(key);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(key + " is required");
        }
        return value.trim();
    }

    private static Set<String> csv(String value) {
        if (value == null || value.isBlank()) {
            return Set.of();
        }
        return java.util.Arrays.stream(value.split(","))
                .map(String::trim)
                .filter(entry -> !entry.isEmpty())
                .collect(java.util.stream.Collectors.toUnmodifiableSet());
    }

    private static long positiveLong(
            Map<String, String> config, String key, long defaultValue) {
        final long parsed;
        try {
            parsed = Long.parseLong(config.getOrDefault(
                    key, Long.toString(defaultValue)));
        } catch (NumberFormatException failure) {
            throw new IllegalArgumentException(key + " must be an integer", failure);
        }
        if (parsed <= 0) {
            throw new IllegalArgumentException(key + " must be positive");
        }
        return parsed;
    }

    private static int positiveInt(
            Map<String, String> config, String key, int defaultValue) {
        long value = positiveLong(config, key, defaultValue);
        if (value > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(key + " exceeds the integer limit");
        }
        return Math.toIntExact(value);
    }

    private record BlobRuntime(
            GovernedBlobStore store, HttpsBlobImporter importer) implements AutoCloseable {
        private BlobRuntime {
            java.util.Objects.requireNonNull(store, "store must not be null");
            java.util.Objects.requireNonNull(importer, "importer must not be null");
        }

        @Override
        public void close() throws Exception {
            Exception failure = null;
            try {
                importer.close();
            } catch (Exception closeFailure) {
                failure = closeFailure;
            }
            try {
                store.close();
            } catch (Exception closeFailure) {
                if (failure == null) {
                    failure = closeFailure;
                } else {
                    failure.addSuppressed(closeFailure);
                }
            }
            if (failure != null) {
                throw failure;
            }
        }
    }
}

package org.pangrova.worker.ai.provider;

import org.junit.jupiter.api.Test;
import org.pangrova.spi.ai.AiCapabilityDescriptor;
import org.pangrova.spi.ai.AiInvocationException;
import org.pangrova.spi.ai.inference.GenerationParameters;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.pangrova.worker.ai.provider.AiProviderTestConfigurations.inferenceModelConfig;

class ConfiguredAiProviderRegistryTest {

    @Test
    void parsesMultipleVersionedModelsAndResolvesSecretsLazily() {
        AtomicInteger secretResolutions = new AtomicInteger();
        var config = Map.ofEntries(
                Map.entry("models.chat.provider", "openai-compatible"),
                Map.entry("models.chat.provider-model", "chat-deployment:2026-08-01"),
                Map.entry("models.chat.version", "2026-08-01"),
                Map.entry("models.chat.endpoint", "https://models.example/v1/chat/completions"),
                Map.entry("models.chat.secret-reference", "env://CHAT_KEY"),
                Map.entry("models.chat.capabilities", "inference"),
                Map.entry("models.chat.max-batch-size", "8"),
                Map.entry("models.chat.max-input-tokens", "128000"),
                Map.entry("models.chat.max-output-tokens", "8192"),
                Map.entry("models.chat.max-media-bytes", "20971520"),
                Map.entry("models.chat.max-concurrency", "4"),
                Map.entry("models.chat.max-provider-attempts-per-task", "4096"),
                Map.entry("models.chat.max-request-bytes", "262144"),
                Map.entry("models.chat.max-response-bytes", "1048576"),
                Map.entry("models.chat.max-provider-items-per-task", "1024"),
                Map.entry("models.embed.provider", "openai-compatible"),
                Map.entry("models.embed.provider-model", "embed-deployment-v3"),
                Map.entry("models.embed.version", "3"),
                Map.entry("models.embed.endpoint", "https://models.example/v1/embeddings"),
                Map.entry("models.embed.secret-reference", "env://EMBED_KEY"),
                Map.entry("models.embed.capabilities", "embedding"),
                Map.entry("models.embed.max-input-tokens", "8192"),
                Map.entry("models.embed.max-concurrency", "8"),
                Map.entry("models.embed.max-provider-attempts-per-task", "16384"),
                Map.entry("models.embed.max-request-bytes", "262144"),
                Map.entry("models.embed.max-response-bytes", "1048576"),
                Map.entry("models.embed.max-provider-items-per-task", "4096"),
                Map.entry("models.embed.embedding-dimension", "1536"));

        try (var registry = new ConfiguredAiProviderRegistry(config, reference -> {
            secretResolutions.incrementAndGet();
            return "resolved-key";
        })) {
            assertThat(registry.models()).hasSize(2);
            assertThat(secretResolutions).hasValue(0);

            var inference = registry.resolveInference("chat");
            assertThat(inference.modelName()).isEqualTo("chat");
            assertThat(inference.modelVersion()).isEqualTo("2026-08-01");
            assertThat(inference.maxBatchSize()).isEqualTo(8);
            assertThat(inference.capabilities())
                    .extracting(AiCapabilityDescriptor::operation)
                    .containsExactly(AiCapabilityDescriptor.Operation.TEXT_GENERATION);
            assertThat(inference.capabilities().iterator().next().retryPolicy())
                    .isEqualTo(AiCapabilityDescriptor.RetryPolicy.noRetry());
            assertThat(registry.models().stream()
                    .filter(model -> model.name().equals("chat"))
                    .findFirst().orElseThrow().properties())
                    .containsEntry("provider-model", "chat-deployment:2026-08-01");

            var embedding = registry.resolveEmbedding("embed");
            assertThat(embedding.modelVersion()).isEqualTo("3");
            assertThat(embedding.dimensions()).isEqualTo(1536);
            assertThat(embedding.capabilities().iterator().next().signatures())
                    .containsExactly(new AiCapabilityDescriptor.Signature(
                            "VECTOR", java.util.List.of("VARCHAR")));
            assertThat(embedding.capabilities().iterator().next().retryPolicy())
                    .isEqualTo(AiCapabilityDescriptor.RetryPolicy.noRetry());
            assertThat(secretResolutions).hasValue(2);
        }
    }

    @Test
    void unknownModelFailsBeforeSecretResolutionOrProviderCreation() {
        AtomicInteger secretResolutions = new AtomicInteger();
        try (var registry = new ConfiguredAiProviderRegistry(
                inferenceModelConfig(), reference -> {
                    secretResolutions.incrementAndGet();
                    return "key";
                })) {
            assertThatThrownBy(() -> registry.resolveInference("missing"))
                    .isInstanceOfSatisfying(AiInvocationException.class, failure -> {
                        assertThat(failure.category())
                                .isEqualTo(AiInvocationException.Category.MODEL_NOT_FOUND);
                        assertThat(failure.retryable()).isFalse();
                    });
            assertThat(secretResolutions).hasValue(0);
            assertThat(registry.contains("missing")).isFalse();
        }
    }


    @Test
    void rejectsProviderThatCannotBeRegisteredBeforeAdvertisingIt() {
        var config = Map.ofEntries(
                Map.entry("models.chat.provider", "unknown-provider"),
                Map.entry("models.chat.provider-model", "chat-deployment"),
                Map.entry("models.chat.version", "1"),
                Map.entry("models.chat.endpoint", "https://models.example/v1/chat/completions"),
                Map.entry("models.chat.secret-reference", "env://KEY"),
                Map.entry("models.chat.capabilities", "inference"),
                Map.entry("models.chat.max-input-tokens", "8192"),
                Map.entry("models.chat.max-output-tokens", "1024"),
                Map.entry("models.chat.max-concurrency", "4"),
                Map.entry("models.chat.max-provider-attempts-per-task", "4096"),
                Map.entry("models.chat.max-request-bytes", "262144"),
                Map.entry("models.chat.max-response-bytes", "1048576"),
                Map.entry("models.chat.max-provider-items-per-task", "1024"));

        assertThatThrownBy(() -> ConfiguredAiProviderRegistry.describeModels(config))
                .hasMessageContaining("Unsupported AI provider");
    }

    @Test
    void rejectsUnknownPropertiesAndMissingRequiredFields() {
        assertThatThrownBy(() -> new ConfiguredAiProviderRegistry(
                Map.of("models.chat.typo", "value"), reference -> "key"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unknown property");

        assertThatThrownBy(() -> new ConfiguredAiProviderRegistry(
                Map.of(
                        "models.chat.provider", "openai-compatible",
                        "models.chat.provider-model", "chat-deployment",
                        "models.chat.version", "1"),
                reference -> "key"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("endpoint");
    }

    @Test
    void rejectsCapabilityMismatchWithoutCreatingProvider() {
        var config = Map.ofEntries(
                Map.entry("models.embed.provider", "openai-compatible"),
                Map.entry("models.embed.provider-model", "embed-deployment"),
                Map.entry("models.embed.version", "1"),
                Map.entry("models.embed.endpoint", "https://models.example/v1/embeddings"),
                Map.entry("models.embed.secret-reference", "env://KEY"),
                Map.entry("models.embed.capabilities", "embedding"),
                Map.entry("models.embed.max-input-tokens", "8192"),
                Map.entry("models.embed.max-concurrency", "4"),
                Map.entry("models.embed.max-provider-attempts-per-task", "4096"),
                Map.entry("models.embed.max-request-bytes", "262144"),
                Map.entry("models.embed.max-response-bytes", "1048576"),
                Map.entry("models.embed.max-provider-items-per-task", "1024"),
                Map.entry("models.embed.embedding-dimension", "384"));

        try (var registry = new ConfiguredAiProviderRegistry(config, reference -> "key")) {
            assertThatThrownBy(() -> registry.resolveInference("embed"))
                    .hasMessageContaining("no inference capability");
        }
    }

    @Test
    void disabledModelsAreNotPublished() {
        var config = Map.of(
                "models.chat.provider", "openai-compatible",
                "models.chat.provider-model", "chat-deployment",
                "models.chat.version", "1",
                "models.chat.endpoint", "https://models.example/v1/chat/completions",
                "models.chat.secret-reference", "env://KEY",
                "models.chat.capabilities", "inference",
                "models.chat.enabled", "false");

        try (var registry = new ConfiguredAiProviderRegistry(config, reference -> "key")) {
            assertThat(registry.models()).isEmpty();
            assertThat(registry.contains("chat")).isFalse();
        }
    }

    @Test
    void classifiesMissingCredentialAsNonRetryableAuthenticationFailure() {
        try (var registry = new ConfiguredAiProviderRegistry(
                inferenceModelConfig(), reference -> null)) {
            assertThatThrownBy(() -> registry.resolveInference("chat"))
                    .isInstanceOf(AiInvocationException.class)
                    .satisfies(failure -> {
                        AiInvocationException invocation =
                                (AiInvocationException) failure;
                        assertThat(invocation.category()).isEqualTo(
                                AiInvocationException.Category.AUTHENTICATION);
                        assertThat(invocation.retryable()).isFalse();
                    });
        }
    }

    @Test
    void publicModelSignatureExcludesCredentialsButCoversRouting() {
        var base = new java.util.LinkedHashMap<>(Map.ofEntries(
                Map.entry("models.rerank.provider", "openai-compatible"),
                Map.entry("models.rerank.provider-model", "rerank-deployment-v1"),
                Map.entry("models.rerank.version", "v1"),
                Map.entry("models.rerank.endpoint", "https://models.example/v1/rerank"),
                Map.entry("models.rerank.secret-reference", "env://KEY_A"),
                Map.entry("models.rerank.capabilities", "reranking"),
                Map.entry("models.rerank.max-input-tokens", "8192"),
                Map.entry("models.rerank.max-concurrency", "4"),
                Map.entry("models.rerank.max-provider-attempts-per-task", "4096"),
                Map.entry("models.rerank.max-request-bytes", "262144"),
                Map.entry("models.rerank.max-response-bytes", "1048576"),
                Map.entry("models.rerank.max-provider-items-per-task", "1024")));
        var first = ConfiguredAiProviderRegistry.describeModels(base)
                .iterator().next();
        assertThat(first.capabilities().get(0).retryPolicy())
                .isEqualTo(AiCapabilityDescriptor.RetryPolicy.noRetry());
        base.put("models.rerank.max-provider-items-per-task", "1025");
        var invocationLimitChange = ConfiguredAiProviderRegistry
                .describeModels(base).iterator().next();
        base.put("models.rerank.max-provider-items-per-task", "1024");
        base.put("models.rerank.secret-reference", "env://KEY_B");
        var credentialRotation = ConfiguredAiProviderRegistry.describeModels(base)
                .iterator().next();
        base.put("models.rerank.endpoint", "https://other.example/v1/rerank");
        var routeChange = ConfiguredAiProviderRegistry.describeModels(base)
                .iterator().next();
        base.put("models.rerank.provider-model", "rerank-deployment-v2");
        var deploymentChange = ConfiguredAiProviderRegistry.describeModels(base)
                .iterator().next();

        assertThat(org.pangrova.spi.ai.AiModelIdentity.signatureSha256(first))
                .containsExactly(org.pangrova.spi.ai.AiModelIdentity
                        .signatureSha256(credentialRotation));
        assertThat(org.pangrova.spi.ai.AiModelIdentity.signatureSha256(first))
                .isNotEqualTo(org.pangrova.spi.ai.AiModelIdentity
                        .signatureSha256(invocationLimitChange));
        assertThat(org.pangrova.spi.ai.AiModelIdentity.signatureSha256(first))
                .isNotEqualTo(org.pangrova.spi.ai.AiModelIdentity
                        .signatureSha256(routeChange));
        assertThat(org.pangrova.spi.ai.AiModelIdentity.signatureSha256(routeChange))
                .isNotEqualTo(org.pangrova.spi.ai.AiModelIdentity
                        .signatureSha256(deploymentChange));
    }

    @Test
    void publicModelSignatureCanonicalizesEquivalentDefaultsAndOrdering() {
        var implicit = Map.ofEntries(
                Map.entry("models.rerank.provider", "openai-compatible"),
                Map.entry("models.rerank.provider-model", "rerank-deployment-v1"),
                Map.entry("models.rerank.version", "v1"),
                Map.entry("models.rerank.endpoint", "https://models.example/a/../v1/rerank"),
                Map.entry("models.rerank.secret-reference", "env://KEY"),
                Map.entry("models.rerank.capabilities", "reranking"),
                Map.entry("models.rerank.max-input-tokens", "8192"),
                Map.entry("models.rerank.max-concurrency", "4"),
                Map.entry("models.rerank.max-provider-attempts-per-task", "4096"),
                Map.entry("models.rerank.max-request-bytes", "262144"),
                Map.entry("models.rerank.max-response-bytes", "1048576"),
                Map.entry("models.rerank.max-provider-items-per-task", "1024"));
        var explicit = Map.ofEntries(
                Map.entry("models.rerank.provider", "openai-compatible"),
                Map.entry("models.rerank.provider-model", "rerank-deployment-v1"),
                Map.entry("models.rerank.version", "v1"),
                Map.entry("models.rerank.endpoint", "https://models.example/v1/rerank"),
                Map.entry("models.rerank.secret-reference", "env://OTHER_KEY"),
                Map.entry("models.rerank.capabilities", " reranking "),
                Map.entry("models.rerank.max-input-tokens", "8192"),
                Map.entry("models.rerank.max-concurrency", "4"),
                Map.entry("models.rerank.max-provider-attempts-per-task", "4096"),
                Map.entry("models.rerank.max-request-bytes", "262144"),
                Map.entry("models.rerank.max-response-bytes", "1048576"),
                Map.entry("models.rerank.max-provider-items-per-task", "1024"),
                Map.entry("models.rerank.max-batch-size", "16"),
                Map.entry("models.rerank.max-requests-per-second", "10.00"),
                Map.entry("models.rerank.timeout-seconds", "030"),
                Map.entry("models.rerank.enabled", "true"));

        var first = ConfiguredAiProviderRegistry.describeModels(implicit)
                .iterator().next();
        var second = ConfiguredAiProviderRegistry.describeModels(explicit)
                .iterator().next();

        assertThat(org.pangrova.spi.ai.AiModelIdentity.signatureSha256(first))
                .containsExactly(org.pangrova.spi.ai.AiModelIdentity
                        .signatureSha256(second));
    }

    @Test
    void embeddingDimensionIsValidatedBeforeCapabilityAdvertisement() {
        var config = Map.ofEntries(
                Map.entry("models.embed.provider", "openai-compatible"),
                Map.entry("models.embed.provider-model", "embed-deployment"),
                Map.entry("models.embed.version", "v1"),
                Map.entry("models.embed.endpoint", "https://models.example/v1/embeddings"),
                Map.entry("models.embed.secret-reference", "env://KEY"),
                Map.entry("models.embed.capabilities", "embedding"),
                Map.entry("models.embed.max-input-tokens", "8192"),
                Map.entry("models.embed.max-concurrency", "4"),
                Map.entry("models.embed.max-provider-attempts-per-task", "4096"),
                Map.entry("models.embed.max-request-bytes", "262144"),
                Map.entry("models.embed.max-response-bytes", "1048576"),
                Map.entry("models.embed.max-provider-items-per-task", "1024"));

        assertThatThrownBy(() -> ConfiguredAiProviderRegistry.describeModels(config))
                .hasMessageContaining("embedding-dimension");
    }

    @Test
    void explicitAnonymousEndpointDoesNotResolveOrAdvertiseASecret() {
        AtomicInteger secretResolutions = new AtomicInteger();
        var config = Map.ofEntries(
                Map.entry("models.local.provider", "openai-compatible"),
                Map.entry("models.local.provider-model", "tinyllama:pinned-e2e"),
                Map.entry("models.local.version", "sha256:test-model"),
                Map.entry("models.local.endpoint", "http://localhost:11434/v1/chat/completions"),
                Map.entry("models.local.authentication", "none"),
                Map.entry("models.local.capabilities", "inference"),
                Map.entry("models.local.max-input-tokens", "2048"),
                Map.entry("models.local.max-output-tokens", "256"),
                Map.entry("models.local.max-concurrency", "2"),
                Map.entry("models.local.max-provider-attempts-per-task", "128"),
                Map.entry("models.local.max-request-bytes", "262144"),
                Map.entry("models.local.max-response-bytes", "1048576"),
                Map.entry("models.local.max-provider-items-per-task", "32"));

        try (var registry = new ConfiguredAiProviderRegistry(config, reference -> {
            secretResolutions.incrementAndGet();
            return "unexpected";
        })) {
            var model = registry.models().iterator().next();
            assertThat(model.secretReference()).isEmpty();
            assertThat(model.properties())
                    .containsEntry("authentication", "none")
                    .containsEntry("provider-model", "tinyllama:pinned-e2e");
            assertThat(registry.resolveInference("local").modelName()).isEqualTo("local");
            assertThat(secretResolutions).hasValue(0);
        }

        var contradictory = new java.util.LinkedHashMap<>(config);
        contradictory.put("models.local.secret-reference", "env://UNUSED");
        assertThatThrownBy(() -> ConfiguredAiProviderRegistry.describeModels(contradictory))
                .hasMessageContaining("must not configure a secret-reference");
    }

    @Test
    void rejectsCredentialsOrRemoteTrafficOverPlainHttp() {
        var authenticated = new java.util.LinkedHashMap<>(inferenceModelConfig());
        authenticated.put(
                "models.chat.endpoint", "http://models.example/v1/chat/completions");
        assertThatThrownBy(() -> ConfiguredAiProviderRegistry.describeModels(authenticated))
                .hasMessageContaining("must use HTTPS");

        var anonymous = new java.util.LinkedHashMap<>(authenticated);
        anonymous.put("models.chat.authentication", "none");
        anonymous.remove("models.chat.secret-reference");
        assertThatThrownBy(() -> ConfiguredAiProviderRegistry.describeModels(anonymous))
                .hasMessageContaining("anonymous loopback");
    }

    @Test
    void closeWaitsForInFlightResolutionAndClosesTheCreatedProvider() throws Exception {
        CountDownLatch secretRequested = new CountDownLatch(1);
        CountDownLatch releaseSecret = new CountDownLatch(1);
        var registry = new ConfiguredAiProviderRegistry(
                inferenceModelConfig(), reference -> {
                    secretRequested.countDown();
                    await(releaseSecret);
                    return "resolved-key";
                });
        var executor = Executors.newFixedThreadPool(2);
        try {
            var resolving = CompletableFuture.supplyAsync(
                    () -> registry.resolveInference("chat"), executor);
            assertThat(secretRequested.await(5, TimeUnit.SECONDS)).isTrue();

            CountDownLatch closeStarted = new CountDownLatch(1);
            CountDownLatch closeReturned = new CountDownLatch(1);
            var closing = CompletableFuture.runAsync(() -> {
                closeStarted.countDown();
                try {
                    registry.close();
                } finally {
                    closeReturned.countDown();
                }
            }, executor);
            assertThat(closeStarted.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(closeReturned.await(100, TimeUnit.MILLISECONDS)).isFalse();

            releaseSecret.countDown();
            var provider = resolving.get(5, TimeUnit.SECONDS);
            closing.get(5, TimeUnit.SECONDS);

            // Batch dispatch wraps submission failures after all accepted work drains.
            assertThatThrownBy(() -> provider.completeBatch(
                    List.of("first", "second"), GenerationParameters.DEFAULTS))
                    .isInstanceOf(java.util.concurrent.CompletionException.class)
                    .hasRootCauseInstanceOf(RejectedExecutionException.class);
            assertThatThrownBy(() -> registry.resolveInference("chat"))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("closed");
        } finally {
            releaseSecret.countDown();
            registry.close();
            executor.shutdownNow();
        }
    }


    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while resolving test secret", failure);
        }
    }
}

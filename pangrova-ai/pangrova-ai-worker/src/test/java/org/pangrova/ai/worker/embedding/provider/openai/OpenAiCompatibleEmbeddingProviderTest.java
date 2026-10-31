package org.pangrova.ai.worker.embedding.provider.openai;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.pangrova.ai.api.error.AiInvocationException;
import org.pangrova.ai.api.embedding.BlobReference;
import org.pangrova.ai.api.embedding.EmbeddingBatchRequest;
import org.pangrova.ai.api.embedding.EmbeddingPurpose;
import org.pangrova.ai.worker.context.AiProviderCallContext;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Pure production codec checks; real transport/fault tests use Ollama and Toxiproxy. */
class OpenAiCompatibleEmbeddingProviderTest {

    @Test
    void rawSingletonKeepsOriginalText() {
        assertThat(provider(3, 10).prepareInputs(EmbeddingBatchRequest.text(
                EmbeddingPurpose.RAW, List.of("hello")))).containsExactly("hello");
    }

    @Test
    void purposeIsIndependentOfBatchCardinality() {
        for (EmbeddingPurpose purpose : EmbeddingPurpose.values()) {
            String prefix = switch (purpose) {
                case RAW -> "";
                case QUERY -> "query: ";
                case DOCUMENT -> "doc: ";
            };
            assertThat(provider(3, 10).prepareInputs(EmbeddingBatchRequest.text(
                    purpose, List.of("first", "second"))))
                    .containsExactly(prefix + "first", prefix + "second");
            assertThat(provider(3, 10).prepareInputs(EmbeddingBatchRequest.text(
                    purpose, List.of("first")))).containsExactly(prefix + "first");
        }
    }

    @Test
    void parsesSingleVectorCoordinates() {
        assertThat(provider(3, 10).parseResponse(
                "{\"data\":[{\"embedding\":[0.1,0.2,0.3],\"index\":0}]}", 1, "")
                .vectors().get(0)).containsExactly(0.1f, 0.2f, 0.3f);
    }

    @Test
    void carriesVectorAndRequestIdentityWithoutUsageStatistics() {
        var result = provider(3, 10).parseResponse(
                "{\"data\":[{\"embedding\":[1,2,3],\"index\":0}],"
                        + "\"usage\":{\"prompt_tokens\":5,\"total_tokens\":5}}", 1, "request-7");
        assertThat(result.vectors().get(0)).containsExactly(1, 2, 3);
        assertThat(result.providerRequestId()).isEqualTo("request-7");
    }

    @Test
    void absentProviderRequestIdentityIsNotInvented() {
        var result = provider(3, 10).parseResponse(
                "{\"data\":[{\"embedding\":[1,2,3],\"index\":0}]}", 1, "");
        assertThat(result.providerRequestId()).isEmpty();
    }

    @Test
    void optionalUsageStatisticsDoNotChangeTheVectorContract() {
        assertThat(provider(3, 10).parseResponse(
                "{\"data\":[{\"embedding\":[1,2,3],\"index\":0}],"
                        + "\"usage\":{\"prompt_tokens\":5,\"total_tokens\":4}}", 1, "").vectors().get(0))
                .containsExactly(1, 2, 3);
    }

    @Test
    void restoresProviderResultsToInputIndexOrder() {
        var results = provider(3, 10).parseResponse("{\"data\":["
                + "{\"embedding\":[4,5,6],\"index\":1},"
                + "{\"embedding\":[1,2,3],\"index\":0}]}", 2, "").vectors();
        assertThat(results).hasSize(2);
        assertThat(results.get(0)).containsExactly(1, 2, 3);
        assertThat(results.get(1)).containsExactly(4, 5, 6);
    }

    @Test
    void rejectsDuplicateOrExtraProviderIndexes() {
        for (int secondIndex : List.of(0, 2, -1)) {
            assertThatThrownBy(() -> provider(3, 10).parseResponse("{\"data\":["
                    + "{\"embedding\":[1,2,3],\"index\":0},"
                    + "{\"embedding\":[4,5,6],\"index\":" + secondIndex + "}]}", 2, ""))
                    .isInstanceOf(AiInvocationException.class)
                    .hasMessageContaining("duplicated or out of range");
        }
    }

    @Test
    void rejectsMissingAndNonIntegralIndexes() {
        for (String index : List.of("", ",\"index\":0.5", ",\"index\":2147483648")) {
            assertThatThrownBy(() -> provider(3, 10).parseResponse(
                    "{\"data\":[{\"embedding\":[1,2,3]" + index + "}]}", 1, ""))
                    .isInstanceOf(AiInvocationException.class)
                    .hasMessageContaining("missing or non-integral");
        }
    }

    @Test
    void providerResponsesDoNotAuthorizePostReplay() {
        for (int status : List.of(408, 429, 500, 503, 504, 599)) {
            assertThat(OpenAiCompatibleEmbeddingProvider.responseFailure(status).retryable()).isFalse();
        }
        assertThat(OpenAiCompatibleEmbeddingProvider.responseFailure(429).category())
                .isEqualTo(AiInvocationException.Category.RATE_LIMIT);
    }

    @Test
    void authenticationAndAuthorizationFailuresAreTerminal() {
        assertThat(OpenAiCompatibleEmbeddingProvider.responseFailure(401).category())
                .isEqualTo(AiInvocationException.Category.AUTHENTICATION);
        assertThat(OpenAiCompatibleEmbeddingProvider.responseFailure(403).category())
                .isEqualTo(AiInvocationException.Category.AUTHORIZATION);
        for (int status : List.of(400, 401, 403, 404)) {
            assertThat(OpenAiCompatibleEmbeddingProvider.responseFailure(status).retryable()).isFalse();
        }
    }

    @Test
    void timeoutFailuresHaveExplicitCategory() {
        for (int status : List.of(408, 504)) {
            assertThat(OpenAiCompatibleEmbeddingProvider.responseFailure(status).category())
                    .isEqualTo(AiInvocationException.Category.TIMEOUT);
        }
    }

    @Test
    void invalidResponseHandling() {
        assertThatThrownBy(() -> provider(3, 10).parseResponse("{\"data\":[]}", 1, ""))
                .isInstanceOf(AiInvocationException.class).hasMessageContaining("expected 1 embeddings");
        assertThatThrownBy(() -> provider(3, 10).parseResponse("not json", 1, ""))
                .isInstanceOf(AiInvocationException.class).hasMessageContaining("not valid JSON");
        for (String body : List.of("", "null", "[]", "1")) {
            assertThatThrownBy(() -> provider(3, 10).parseResponse(body, 1, ""))
                    .isInstanceOf(AiInvocationException.class)
                    .hasMessageContaining("must be a JSON object");
        }
    }

    @Test
    void dimensionMismatchThrows() {
        assertThatThrownBy(() -> provider(3, 10).parseResponse(
                "{\"data\":[{\"embedding\":[0.1,0.2],\"index\":0}]}", 1, ""))
                .isInstanceOf(AiInvocationException.class).hasMessageContaining("Dimension mismatch");
    }

    @Test
    void rejectsNonNumericOrNonFiniteCoordinates() {
        for (String coordinate : List.of("null", "\"text\"", "1e100")) {
            assertThatThrownBy(() -> provider(3, 10).parseResponse(
                    "{\"data\":[{\"embedding\":[1,2," + coordinate + "],\"index\":0}]}", 1, ""))
                    .isInstanceOf(AiInvocationException.class).extracting("category")
                    .isEqualTo(AiInvocationException.Category.PROVIDER_RESPONSE_INVALID);
        }
    }

    @Test
    void nullRequestThrowsBeforeProviderIo() {
        assertThatThrownBy(() -> provider(3, 10).embed(null))
                .isInstanceOf(NullPointerException.class).hasMessageContaining("request required");
    }

    @Test
    void emptyBatchThrows() {
        assertThatThrownBy(() -> EmbeddingBatchRequest.text(EmbeddingPurpose.RAW, List.of()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("empty");
    }

    @Test
    void nullBatchEntryThrowsBeforeProviderIo() {
        assertThatThrownBy(() -> EmbeddingBatchRequest.text(
                EmbeddingPurpose.RAW, java.util.Arrays.asList("valid", null)))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void batchExceedingMaxSizeThrows() {
        assertThatThrownBy(() -> provider(3, 2).embed(EmbeddingBatchRequest.text(
                EmbeddingPurpose.RAW, List.of("a", "b", "c"))))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("exceeds maxBatchSize");
    }

    @Test
    void jsonSpecialCharactersEscaped() throws Exception {
        String text = "text with \"quotes\" and \nnewlines\\汉字";
        String body = provider(3, 10).buildRequestJson(List.of(text));
        assertThat(body).contains("\\\"", "\\n", "\\\\");
        var parsed = new ObjectMapper().readTree(body);
        assertThat(parsed.path("model").asText()).isEqualTo("test-model");
        assertThat(parsed.path("input").get(0).asText()).isEqualTo(text);
    }

    @Test
    void authorizationAndIdempotencyHeadersSent() {
        AiProviderCallContext.callWith("identity", Instant.now().plusSeconds(10), () -> {
            var request = provider(3, 10).prepareHttpRequest(List.of("auth text"));
            assertThat(request.headers().firstValue("Authorization")).contains("Bearer test-key");
            assertThat(request.headers().firstValue("Idempotency-Key")).contains("identity:request-0");
            assertThat(request.timeout().orElseThrow()).isLessThanOrEqualTo(Duration.ofSeconds(10));
            return null;
        });
    }

    @Test
    void anonymousEndpointOmitsAuthorizationHeader() {
        var provider = new OpenAiCompatibleEmbeddingProvider(new OpenAiEmbeddingConfig(
                "http://localhost:11434/v1/embeddings", "local-pinned", "", 3, 10,
                1000.0, "", "", 30));
        assertThat(provider.prepareHttpRequest(List.of("anonymous"))
                .headers().firstValue("Authorization")).isEmpty();
    }

    @Test
    void modelNameAndDimensionsAccessors() {
        var provider = provider(768, 32);
        assertThat(provider.modelName()).isEqualTo("test-model");
        assertThat(provider.dimensions()).isEqualTo(768);
        assertThat(provider.maxBatchSize()).isEqualTo(32);
        assertThat(provider.supportedPurposes()).containsExactlyInAnyOrder(EmbeddingPurpose.values());
    }

    @Test
    void rejectsBlobEmbeddingBecauseEndpointContractIsTextOnly() {
        String checksum = "a".repeat(64);
        BlobReference reference = new BlobReference("pangrova-blob://v2/" + "b".repeat(64)
                + "/" + "c".repeat(64) + "/" + checksum, "image/png", 4, checksum);
        assertThatThrownBy(() -> provider(3, 10).embed(EmbeddingBatchRequest.blob(
                EmbeddingPurpose.DOCUMENT, List.of(reference))))
                .isInstanceOf(AiInvocationException.class).extracting("category")
                .isEqualTo(AiInvocationException.Category.CAPABILITY_MISMATCH);
    }

    private static OpenAiCompatibleEmbeddingProvider provider(int dimension, int maxBatchSize) {
        return new OpenAiCompatibleEmbeddingProvider(new OpenAiEmbeddingConfig(
                "http://localhost:11434/v1/embeddings", "test-model", "test-key", dimension,
                maxBatchSize, 1000.0, "query: ", "doc: ", 30));
    }
}

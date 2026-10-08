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
package org.pangrova.testing.e2e.ai;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.pangrova.spi.connector.data.ColumnValue;
import org.pangrova.testing.E2ETest;
import org.pangrova.testing.e2e.PangrovaE2ETestBase;
import org.pangrova.testing.e2e.config.ConnectorTestConfig;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.pangrova.testing.e2e.ai.SharedAiProvider.DOCUMENT_PREFIX;
import static org.pangrova.testing.e2e.ai.SharedAiProvider.EMBEDDING_DIMENSION;
import static org.pangrova.testing.e2e.ai.SharedAiProvider.EMBEDDING_LOGICAL_MODEL;
import static org.pangrova.testing.e2e.ai.SharedAiProvider.EMBEDDING_PROVIDER_MODEL;
import static org.pangrova.testing.e2e.ai.SharedAiProvider.LOGICAL_MODEL;
import static org.pangrova.testing.e2e.ai.SharedAiProvider.QUERY_PREFIX;

/**
 * Real-provider AI SQL acceptance test.
 *
 * <p>This test deliberately does not install a fake {@code InferenceProvider}.
 * SQL is submitted over Coordinator HTTP, planned and bound against the exact
 * Worker capability inventory, dispatched through the canonical task protocol,
 * evaluated by the Native engine through JNI, and finally invokes an isolated
 * Ollama OpenAI-compatible endpoint from the Worker callback.</p>
 */
@E2ETest
@Tag("ai-provider")
@DisplayName("Ollama AI SQL full-chain E2E")
class OllamaAiSqlE2ETest extends PangrovaE2ETestBase {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final HttpClient HTTP = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .build();

    @BeforeAll
    static void registerRealEmbeddingModel() {
        String createEmbeddingModel = "CREATE EMBEDDING MODEL \""
                + EMBEDDING_LOGICAL_MODEL + "\" ("
                + "provider = 'openai-compatible', "
                + "endpoint = '" + endpoint("/v1/embeddings") + "', "
                + "dimension = " + EMBEDDING_DIMENSION + ", "
                + "authentication = 'none', "
                + "query_prefix = '" + QUERY_PREFIX + "', "
                + "document_prefix = '" + DOCUMENT_PREFIX + "', "
                + "max_batch_size = 8, "
                + "max_requests_per_second = 32)";
        var created = framework.executeQuery(createEmbeddingModel);
        assertThat(created.success())
                .as("Public SQL must register the real embedding deployment: %s",
                        created.errorMessage())
                .isTrue();
    }

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of();
    }

    @Test
    @DisplayName("AI_COMPLETE crosses Coordinator, Worker, JNI, Native and real provider")
    void aiCompleteRunsThroughTheRealDistributedSqlPipeline() {
        long before = completionRequestCount();
        var rows = executeQuery("SELECT AI_COMPLETE('" + LOGICAL_MODEL
                + "', 'Reply with one short greeting.') AS response");

        assertThat(rows).hasSize(1);
        Object value = ColumnValue.toRaw(rows.get(0).values().values().iterator().next());
        assertThat(value).isInstanceOf(String.class);
        assertThat((String) value).isNotBlank();
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                assertThat(completionRequestCount()).isEqualTo(before + 1));
    }

    @Test
    @DisplayName("non-deterministic AI_COMPLETE performs a real provider call on each execution")
    void repeatedAiCompleteInvokesTheProviderForEachExecution() {
        String sql = "SELECT AI_COMPLETE('" + LOGICAL_MODEL
                + "', 'Reply with exactly this cache marker: pangrova-r7-cache.') AS response";
        long before = completionRequestCount();

        String first = scalarString(executeQuery(sql));
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                assertThat(completionRequestCount()).isEqualTo(before + 1));
        long afterFirst = completionRequestCount();

        String second = scalarString(executeQuery(sql));
        assertThat(first).isNotBlank();
        assertThat(second).isNotBlank();
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                assertThat(completionRequestCount()).isEqualTo(afterFirst + 1));
    }

    @Test
    @DisplayName("unadvertised model fails closed before provider invocation")
    void unknownModelCannotBeScheduled() {
        long before = completionRequestCount();
        assertThat(executeExpectingError(
                "SELECT AI_COMPLETE('unadvertised-model', 'must not run')"))
                .containsIgnoringCase("model")
                .containsIgnoringCase("capability");
        assertThat(completionRequestCount()).isEqualTo(before);
    }

    @Test
    @DisplayName("AI_EMBED returns exact real-provider VECTORs for distinct SQL inputs")
    void aiEmbedReturnsRealProviderVectorsForDistinctSqlInputs() throws Exception {
        float[] firstExpected = directEmbedding("Pangrova red apple");
        float[] secondExpected = directEmbedding("Pangrova blue ocean");
        long before = embeddingRequestCount();

        var first = executeQuery("SELECT AI_EMBED('"
                + EMBEDDING_LOGICAL_MODEL
                + "', 'Pangrova red apple') AS embedding");
        var second = executeQuery("SELECT AI_EMBED('"
                + EMBEDDING_LOGICAL_MODEL
                + "', 'Pangrova blue ocean') AS embedding");

        assertThat(first).hasSize(1);
        assertThat(second).hasSize(1);
        assertEmbedding(first.get(0).get("embedding"), firstExpected);
        assertEmbedding(second.get(0).get("embedding"), secondExpected);
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                assertThat(embeddingRequestCount()).isEqualTo(before + 2));
    }

    @Test
    @DisplayName("AI_EMBED rejects unadvertised and non-embedding models before provider access")
    void aiEmbedFailsClosedForWrongModelCapability() {
        long before = embeddingRequestCount();

        assertThat(executeExpectingError(
                "SELECT AI_EMBED('unadvertised-model', 'must not run')"))
                .containsIgnoringCase("model");
        assertThat(executeExpectingError(
                "SELECT AI_EMBED('" + LOGICAL_MODEL + "', 'must not run')"))
                .containsIgnoringCase("not registered in the catalog");
        assertThat(embeddingRequestCount()).isEqualTo(before);
    }

    @Test
    @DisplayName("Embedding purposes retain real-provider semantics through scalar, batch and NULL rows")
    void embeddingPurposeDoesNotDependOnPhysicalBatchCardinality() throws Exception {
        String text = "Pangrova purpose-sensitive text";
        float[] raw = directEmbedding(text);
        float[] query = directEmbedding(QUERY_PREFIX + text);
        float[] document = directEmbedding(DOCUMENT_PREFIX + text);
        assertThat(raw).isNotEqualTo(query).isNotEqualTo(document);
        assertThat(query).isNotEqualTo(document);
        for (String purpose : List.of("RAW", "QUERY", "DOCUMENT")) {
            float[] expected = switch (purpose) {
                case "RAW" -> raw;
                case "QUERY" -> query;
                case "DOCUMENT" -> document;
                default -> throw new IllegalArgumentException("Unexpected test purpose");
            };
            assertScalarAndBatchPurpose(text, purpose, expected);
        }
    }

    private void assertScalarAndBatchPurpose(String text, String purpose, float[] expected) throws Exception {
        assertEmbedding(executeQuery("SELECT AI_EMBED('" + EMBEDDING_LOGICAL_MODEL
                + "', '" + text + "', '" + purpose + "') AS embedding")
                .get(0).get("embedding"), expected);
        var rows = executeQuery(
                    "SELECT id, AI_EMBED('" + EMBEDDING_LOGICAL_MODEL + "', content, '" + purpose
                            + "') AS embedding FROM (VALUES "
                            + "(1, '" + text + "'), (2, 'second embedding text'),"
                            + "(3, '" + text + "'), (4, '" + text + "'),"
                            + "(5, '" + text + "'), (6, '" + text + "'),"
                            + "(7, '" + text + "'), (8, '" + text + "'),"
                            + "(9, '" + text + "'), (10, NULL)) AS input(id, content) ORDER BY id");
        assertThat(rows).hasSize(10);
        for (int index = 0; index < rows.size(); index++) {
                assertThat(((Number) ColumnValue.toRaw(rows.get(index).get("id"))).intValue())
                        .isEqualTo(index + 1);
                if (index == 9) {
                    assertThat(ColumnValue.toRaw(rows.get(index).get("embedding"))).isNull();
                } else if (index == 1) {
                    String prefix = purpose.equals("RAW") ? ""
                            : purpose.equals("QUERY") ? QUERY_PREFIX : DOCUMENT_PREFIX;
                    assertEmbedding(rows.get(index).get("embedding"),
                            directEmbedding(prefix + "second embedding text"));
                } else {
                    assertEmbedding(rows.get(index).get("embedding"), expected);
                }
        }
    }

    @Test
    @DisplayName("Unsupported or NULL embedding purpose fails before real provider I/O")
    void invalidEmbeddingPurposeCannotInvokeProvider() {
        long before = embeddingRequestCount();
        assertThat(executeExpectingError("SELECT AI_EMBED('" + EMBEDDING_LOGICAL_MODEL
                + "', 'must not run', 'UNKNOWN')")).containsIgnoringCase("purpose");
        assertThat(executeExpectingError("SELECT AI_EMBED('" + EMBEDDING_LOGICAL_MODEL
                + "', 'must not run', NULL)")).containsIgnoringCase("purpose");
        assertThat(embeddingRequestCount()).isEqualTo(before);
    }

    @Test
    @DisplayName("SQL ASYNC generated values cross the real commit queue and Native artifact publication")
    void asyncGeneratedValuesFollowTheRealCommittedWriteLifecycle() throws Exception {
        String table = "async_generated_" + java.util.UUID.randomUUID().toString().replace("-", "");
        String qualified = "default.default." + table;
        float[] expected = directEmbedding(DOCUMENT_PREFIX + "real durable generated value");
        long before = embeddingRequestCount();
        executeDdl("CREATE TABLE " + qualified + " (id BIGINT, content VARCHAR, embedding VECTOR("
                + EMBEDDING_DIMENSION + ") GENERATED ALWAYS AS (AI_EMBED('" + EMBEDDING_LOGICAL_MODEL
                + "', content)) EMBEDDING MODE ASYNC)");
        try {
            executeDml("INSERT INTO " + qualified + " (id, content) VALUES (1, 'real durable generated value')");
            await().atMost(Duration.ofMinutes(2)).pollInterval(Duration.ofMillis(200)).untilAsserted(() -> {
                GeneratedSqlPublicationAssertions.requirePublished(stateStoreDataSource(), table);
                assertThat(embeddingRequestCount()).isGreaterThan(before);
            });
            assertQueryReturnsRows("SELECT id, content FROM " + qualified, 1);
            var visible = executeQuery("SELECT embedding FROM " + qualified + " WHERE id=1");
            assertThat(visible).hasSize(1);
            assertEmbedding(visible.get(0).get("embedding"), expected);
        } finally {
            executeDdl("DROP TABLE " + qualified);
        }
    }

    @Test
    @DisplayName("NULL ASYNC sources retain their published physical row without an invented embedding")
    void nullGeneratedSourcePublishesAndReadsNullWithoutProviderInvocation() throws Exception {
        String table = "async_null_" + java.util.UUID.randomUUID().toString().replace("-", "");
        String qualified = "default.default." + table;
        long before = embeddingRequestCount();
        executeDdl("CREATE TABLE " + qualified + " (id BIGINT, content VARCHAR, embedding VECTOR("
                + EMBEDDING_DIMENSION + ") GENERATED ALWAYS AS (AI_EMBED('" + EMBEDDING_LOGICAL_MODEL
                + "', content)) EMBEDDING MODE ASYNC)");
        try {
            executeDml("INSERT INTO " + qualified + " (id, content) VALUES (1, NULL)");
            await().atMost(Duration.ofMinutes(2)).pollInterval(Duration.ofMillis(200)).untilAsserted(() ->
                    GeneratedSqlPublicationAssertions.requirePublished(stateStoreDataSource(), table));
            var visible = executeQuery("SELECT id, content, embedding FROM " + qualified);
            assertThat(visible).hasSize(1);
            assertThat(ColumnValue.toRaw(visible.get(0).get("id"))).isEqualTo(1L);
            assertThat(ColumnValue.toRaw(visible.get(0).get("content"))).isNull();
            assertThat(ColumnValue.toRaw(visible.get(0).get("embedding"))).isNull();
            await().during(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(3)).untilAsserted(() ->
                    assertThat(embeddingRequestCount()).isEqualTo(before));
        } finally {
            executeDdl("DROP TABLE " + qualified);
        }
    }

    private static float[] directEmbedding(String input) throws Exception {
        var body = MAPPER.createObjectNode();
        body.put("model", EMBEDDING_PROVIDER_MODEL);
        body.putArray("input").add(input);
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(endpoint("/v1/embeddings")))
                .timeout(Duration.ofSeconds(120))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(MAPPER.writeValueAsString(body)))
                .build();
        HttpResponse<String> response = HTTP.send(
                request, HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode())
                .as("direct Ollama embedding request: %s", response.body())
                .isEqualTo(200);
        JsonNode data = MAPPER.readTree(response.body()).path("data");
        assertThat(data.isArray()).isTrue();
        assertThat(data).hasSize(1);
        assertThat(data.get(0).path("index").asInt()).isZero();
        JsonNode coordinates = data.get(0).path("embedding");
        assertThat(coordinates.isArray()).isTrue();
        assertThat(coordinates).hasSize(EMBEDDING_DIMENSION);
        float[] result = new float[EMBEDDING_DIMENSION];
        for (int i = 0; i < result.length; i++) {
            result[i] = (float) coordinates.get(i).asDouble();
            assertThat(Float.isFinite(result[i])).isTrue();
        }
        return result;
    }

    private static void assertEmbedding(ColumnValue value, float[] expected) {
        assertThat(value).isInstanceOf(ColumnValue.VectorValue.class);
        float[] actual = ((ColumnValue.VectorValue) value).value();
        assertThat(actual).hasSize(EMBEDDING_DIMENSION);
        for (int i = 0; i < actual.length; i++) {
            assertThat(actual[i])
                    .as("embedding coordinate %s", i)
                    .isFinite()
                    .isCloseTo(expected[i], org.assertj.core.data.Offset.offset(1e-5f));
        }
    }

    private static long completionRequestCount() {
        return sharedAiProvider().requestCount("/v1/chat/completions");
    }

    private static long embeddingRequestCount() {
        return sharedAiProvider().requestCount("/v1/embeddings");
    }

    private static String scalarString(List<org.pangrova.spi.connector.data.TypedRow> rows) {
        assertThat(rows).hasSize(1);
        Object value = ColumnValue.toRaw(rows.get(0).values().values().iterator().next());
        assertThat(value).isInstanceOf(String.class);
        return (String) value;
    }

    private static String endpoint(String path) {
        return sharedAiProvider().endpoint(path);
    }
}

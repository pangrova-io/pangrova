package org.pangrova.testing.integration.e2e.client;

import org.junit.jupiter.api.Test;
import org.pangrova.common.client.query.SubmitQueryResponse;
import org.pangrova.common.client.query.QueryStatusResponse;
import org.pangrova.common.enums.ResultDisposition;
import org.pangrova.common.json.CanonicalJsonMapper;
import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** Production authentication, submission, lifecycle, rejection, and receipt replay over HTTP. */
class QuerySubmissionIdempotencyE2ETest extends SqlE2ETestBase {
    private static final HttpClient CLIENT = HttpClient.newHttpClient();
    private static final String SELECT = """
            {"query":"SELECT 1 AS n","session_properties":{}}
            """;

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(ConnectorTestConfig.icebergLocal(
                "iceberg_catalog", "default", icebergWarehouse().toString()));
    }

    @Test
    void replayReturnsOriginalReceiptAndRequiresSamePrincipalAndRequest() throws Exception {
        String key = UUID.randomUUID().toString();
        var first = submit(key, SELECT, E2E_USER_BEARER_TOKEN);
        assertThat(first.statusCode()).as(first.body()).isEqualTo(200);
        var original = CanonicalJsonMapper.readValue(first.body().getBytes(java.nio.charset.StandardCharsets.UTF_8),
                SubmitQueryResponse.class);
        assertThat(original.queryId()).isNotBlank();
        var replay = submit(key, "{\"session_properties\":{},\"query\":\"SELECT 1 AS n\"}", E2E_USER_BEARER_TOKEN);
        assertThat(replay.statusCode()).isEqualTo(200);
        assertThat(replay.body()).isEqualTo(first.body());
        for (String changed : List.of(SELECT.replace("SELECT 1", "SELECT 2"),
                SELECT.replace("{}", "{\"catalog\":\"other\"}"))) {
            var conflict = submit(key, changed, E2E_USER_BEARER_TOKEN);
            assertThat(conflict.statusCode()).isEqualTo(409);
            assertThat(conflict.body()).doesNotContain(original.queryId());
        }
        var denied = submit(key, SELECT, "unrecognized-token");
        assertThat(denied.statusCode()).isEqualTo(401);
        var other = submit(key, SELECT, E2E_SECOND_USER_BEARER_TOKEN);
        assertThat(other.statusCode()).as(other.body()).isEqualTo(200);
        assertThat(other.body()).doesNotContain(original.queryId());
        String otherId = CanonicalJsonMapper.readTree(other.body()).path("query_id").asText();
        assertThat(otherId).isNotBlank().isNotEqualTo(original.queryId());
    }

    @Test
    void retryAfterDrainSubmitsARealQueryAndRecordedInvalidSqlRetainsItsError() throws Exception {
        String key = UUID.randomUUID().toString();
        queryManager.setAcceptingQueries(false);
        try {
            var rejected = submit(key, SELECT, E2E_USER_BEARER_TOKEN);
            assertThat(rejected.statusCode()).isBetween(400, 599);
            assertThat(rejected.headers().firstValue("X-Pangrova-Query-Id")).isEmpty();
        } finally {
            queryManager.setAcceptingQueries(true);
        }
        var accepted = submit(key, SELECT, E2E_USER_BEARER_TOKEN);
        assertThat(accepted.statusCode()).as(accepted.body()).isEqualTo(200);
        String id = CanonicalJsonMapper.readTree(accepted.body()).path("query_id").asText();
        assertThat(queryManager.getQueryState(id)).isPresent();

        String invalidKey = UUID.randomUUID().toString();
        String invalid = "{\"query\":\"SELECT FROM )\"}";
        var first = submit(invalidKey, invalid, E2E_USER_BEARER_TOKEN);
        assertThat(first.statusCode()).as(first.body()).isEqualTo(200);
        var receipt = CanonicalJsonMapper.readValue(first.body().getBytes(java.nio.charset.StandardCharsets.UTF_8),
                SubmitQueryResponse.class);
        String failedId = receipt.queryId();
        assertThat(failedId).isNotBlank();
        assertThat(receipt.state()).isEqualTo("QUEUED");
        assertThat(receipt.resultDisposition()).isEqualTo(ResultDisposition.PENDING);

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            var failed = status(failedId);
            assertThat(failed.state()).isEqualTo("FAILED");
            assertThat(failed.error()).isNotNull();
        });
        var failed = status(failedId);
        var replay = submit(invalidKey, invalid, E2E_USER_BEARER_TOKEN);
        assertThat(replay.statusCode()).as(replay.body()).isEqualTo(200);
        assertThat(replay.body()).isEqualTo(first.body());
        var repeated = status(failedId);
        assertThat(repeated.queryId()).isEqualTo(failedId);
        assertThat(repeated.state()).isEqualTo("FAILED");
        assertThat(repeated.error()).isEqualTo(failed.error());
    }

    private static QueryStatusResponse status(String queryId) throws Exception {
        var request = HttpRequest.newBuilder(URI.create("http://localhost:" + coordinatorHttpPort()
                        + "/v1/query/" + queryId + "/state"))
                .timeout(Duration.ofSeconds(30))
                .header("Authorization", "Bearer " + E2E_USER_BEARER_TOKEN).GET().build();
        var response = CLIENT.send(request, HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).as(response.body()).isEqualTo(200);
        return CanonicalJsonMapper.readValue(response.body().getBytes(java.nio.charset.StandardCharsets.UTF_8),
                QueryStatusResponse.class);
    }

    @Test
    void queriesWithoutKeysGetFreshPendingReceiptsAndCompleteNativeWrites() throws Exception {
        String table = "iceberg_catalog.default.unkeyed_" + UUID.randomUUID().toString().replace("-", "");
        sqlRunner.executeDdl("CREATE TABLE " + table + " (id BIGINT)");
        try {
            var first = submit(null, "{\"query\":\"INSERT INTO " + table + " VALUES (1)\"}", E2E_USER_BEARER_TOKEN);
            assertThat(first.statusCode()).as(first.body()).isEqualTo(200);
            var receipt = CanonicalJsonMapper.readValue(first.body().getBytes(java.nio.charset.StandardCharsets.UTF_8),
                    SubmitQueryResponse.class);
            assertThat(receipt.state()).isEqualTo("QUEUED");
            assertThat(receipt.resultDisposition()).isEqualTo(ResultDisposition.PENDING);
            consumeSubmittedQuery(receipt);

            var distinct = submit(receipt.queryId(), "{\"query\":\"INSERT INTO " + table + " VALUES (2)\"}",
                    E2E_USER_BEARER_TOKEN);
            assertThat(distinct.statusCode()).as(distinct.body()).isEqualTo(200);
            var next = CanonicalJsonMapper.readValue(distinct.body().getBytes(java.nio.charset.StandardCharsets.UTF_8),
                    SubmitQueryResponse.class);
            assertThat(next.queryId()).isNotEqualTo(receipt.queryId());
            consumeSubmittedQuery(next);
            assertThat(sqlRunner.execute("SELECT id FROM " + table + " ORDER BY id"))
                    .extracting(row -> row.getLong("id")).containsExactly(1L, 2L);
        } finally {
            sqlRunner.executeDdl("DROP TABLE " + table);
        }
    }

    private static HttpResponse<String> submit(String key, String body, String token) throws Exception {
        var request = HttpRequest.newBuilder(URI.create("http://localhost:" + coordinatorHttpPort() + "/v1/query/submit"))
                .timeout(Duration.ofSeconds(30)).header("Content-Type", "application/json")
                .header("Authorization", "Bearer " + token)
                .POST(HttpRequest.BodyPublishers.ofString(body));
        if (key != null) request.header("Idempotency-Key", key);
        return CLIENT.send(request.build(), HttpResponse.BodyHandlers.ofString());
    }
}

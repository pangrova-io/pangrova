package org.pangrova.testing.integration.e2e.client;

import org.junit.jupiter.api.Test;
import org.pangrova.common.client.query.SubmitQueryResponse;
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
        assertThat(other.body()).doesNotContain(original.queryId());
        String otherId = other.statusCode() == 200
                ? CanonicalJsonMapper.readTree(other.body()).path("query_id").asText()
                : other.headers().firstValue("X-Pangrova-Query-Id").orElseThrow();
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
        assertThat(first.statusCode()).isBetween(400, 599);
        String failedId = first.headers().firstValue("X-Pangrova-Query-Id").orElseThrow();
        var replay = submit(invalidKey, invalid, E2E_USER_BEARER_TOKEN);
        assertThat(replay.statusCode()).isEqualTo(first.statusCode());
        assertThat(replay.headers().firstValue("X-Pangrova-Query-Id")).contains(failedId);
        assertThat(queryManager.getQueryState(failedId)).isPresent();
    }

    private static HttpResponse<String> submit(String key, String body, String token) throws Exception {
        var request = HttpRequest.newBuilder(URI.create("http://localhost:" + coordinatorHttpPort() + "/v1/query/submit"))
                .timeout(Duration.ofSeconds(30)).header("Content-Type", "application/json")
                .header("Idempotency-Key", key).header("Authorization", "Bearer " + token)
                .POST(HttpRequest.BodyPublishers.ofString(body)).build();
        return CLIENT.send(request, HttpResponse.BodyHandlers.ofString());
    }
}

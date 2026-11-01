package org.pangrova.coordinator.http.endpoint;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.pangrova.common.http.adapter.jetty.EmbeddedHttpServerFactory;
import org.pangrova.common.json.provider.jackson.JsonMapperFactory;
import org.pangrova.coordinator.query.QueryManager;
import org.pangrova.coordinator.query.QueryStateMachine;
import org.pangrova.coordinator.query.stage.StageStateMachine;
import org.pangrova.client.core.error.ErrorDetail;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.Map;

/**
 * Integration test for {@link QueryStatusResource} REST endpoints.
 * Starts an embedded Jetty + Jersey server and verifies the query status
 * listing, detail, and stage APIs return correct JSON responses.
 */
class QueryStatusResourceTest {

    private static Server server;
    private static int port;
    private static final HttpClient HTTP_CLIENT = HttpClient.newHttpClient();
    private static final ObjectMapper MAPPER = JsonMapperFactory.create();
    private static QueryManager queryManager;

    @BeforeAll
    static void startServer() throws Exception {
        queryManager = org.pangrova.coordinator.testinfra.StatusOnlyQueryManagerFixture.create(
                new org.pangrova.coordinator.worker.WorkerManager(20_000, 60_000,
                        new org.pangrova.persistence.memory.coordination.InMemoryWorkerRegistry()),
                new org.pangrova.coordinator.task.CoordinatorTaskTracker());

        ResourceConfig config = new ResourceConfig();
        config.register(new QueryStatusResource(queryManager));
        config.register(JacksonFeature.class);

        ServletContextHandler handler = new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
        handler.setContextPath("/");
        handler.addServlet(new ServletHolder(new ServletContainer(config)), "/*");

        server = EmbeddedHttpServerFactory.create(0, handler);
        server.start();
        port = EmbeddedHttpServerFactory.getPort(server);
    }

    @AfterAll
    static void stopServer() throws Exception {
        if (server != null) {
            server.stop();
        }
    }

    @Test
    void listQueriesReturnsValidStructure() throws Exception {
        HttpResponse<String> response = sendGet("/v1/query-status");

        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.headers().firstValue("content-type").orElse(""))
                .contains("application/json");
        Map<String, Object> body = MAPPER.readValue(response.body(), new TypeReference<>() {});
        assertThat(body).containsKey("totalCount");
        assertThat(body).containsKey("queries");
        assertThat((int) body.get("totalCount")).isGreaterThanOrEqualTo(0);
        assertThat((List<?>) body.get("queries")).isNotNull();
    }

    @Test
    void listQueriesReturnsRegisteredQueries() throws Exception {
        QueryStateMachine sm1 = new QueryStateMachine();
        sm1.setTenantId("user-a");
        sm1.tryTransition(QueryStateMachine.State.PLANNING);
        sm1.tryTransition(QueryStateMachine.State.SCHEDULING);
        sm1.tryTransition(QueryStateMachine.State.RUNNING);
        queryManager.registerStateMachine("status-test-q1", sm1);

        QueryStateMachine sm2 = new QueryStateMachine();
        sm2.setTenantId("user-b");
        sm2.tryTransition(QueryStateMachine.State.PLANNING);
        sm2.tryTransition(QueryStateMachine.State.SCHEDULING);
        sm2.tryTransition(QueryStateMachine.State.RUNNING);
        sm2.tryTransition(QueryStateMachine.State.FINISHING);
        sm2.tryTransition(QueryStateMachine.State.FINISHED);
        queryManager.registerStateMachine("status-test-q2", sm2);

        try {
            HttpResponse<String> response = sendGet("/v1/query-status");
            assertThat(response.statusCode()).isEqualTo(200);

            Map<String, Object> body = MAPPER.readValue(response.body(), new TypeReference<>() {});
            assertThat((int) body.get("totalCount")).isGreaterThanOrEqualTo(2);

            List<Map<String, Object>> queries = (List<Map<String, Object>>) body.get("queries");
            assertThat(queries).isNotEmpty();

            // Check that both queries are present
            Map<String, Object> q1 = queries.stream()
                    .filter(q -> "status-test-q1".equals(q.get("queryId")))
                    .findFirst().orElseThrow();
            assertThat(q1.get("state")).isEqualTo("RUNNING");
            assertThat(q1.get("tenantId")).isEqualTo("user-a");
            assertThat(q1.get("terminal")).isEqualTo(false);
            assertThat(q1.get("elapsedMs")).isNotNull();
            assertThat(q1.get("createdAt")).isNotNull();

            Map<String, Object> q2 = queries.stream()
                    .filter(q -> "status-test-q2".equals(q.get("queryId")))
                    .findFirst().orElseThrow();
            assertThat(q2.get("state")).isEqualTo("FINISHED");
            assertThat(q2.get("terminal")).isEqualTo(true);
        } finally {
            // Cleanup: move sm1 to terminal so it doesn't affect other tests
            sm1.tryTransition(QueryStateMachine.State.FINISHING);
            sm1.tryTransition(QueryStateMachine.State.FINISHED);
        }
    }

    @Test
    void listQueriesWithStateFilter() throws Exception {
        QueryStateMachine smRunning = new QueryStateMachine();
        smRunning.tryTransition(QueryStateMachine.State.PLANNING);
        smRunning.tryTransition(QueryStateMachine.State.SCHEDULING);
        smRunning.tryTransition(QueryStateMachine.State.RUNNING);
        queryManager.registerStateMachine("status-filter-running", smRunning);

        QueryStateMachine smFinished = new QueryStateMachine();
        smFinished.tryTransition(QueryStateMachine.State.PLANNING);
        smFinished.tryTransition(QueryStateMachine.State.SCHEDULING);
        smFinished.tryTransition(QueryStateMachine.State.RUNNING);
        smFinished.tryTransition(QueryStateMachine.State.FINISHING);
        smFinished.tryTransition(QueryStateMachine.State.FINISHED);
        queryManager.registerStateMachine("status-filter-finished", smFinished);

        try {
            HttpResponse<String> response = sendGet("/v1/query-status?state=RUNNING");
            assertThat(response.statusCode()).isEqualTo(200);

            Map<String, Object> body = MAPPER.readValue(response.body(), new TypeReference<>() {});
            List<Map<String, Object>> queries = (List<Map<String, Object>>) body.get("queries");

            // All returned queries must be in RUNNING state
            for (Map<String, Object> q : queries) {
                assertThat(q.get("state")).isEqualTo("RUNNING");
            }

            // The finished query must NOT appear
            assertThat(queries.stream()
                    .anyMatch(q -> "status-filter-finished".equals(q.get("queryId"))))
                    .isFalse();
        } finally {
            smRunning.tryTransition(QueryStateMachine.State.FINISHING);
            smRunning.tryTransition(QueryStateMachine.State.FINISHED);
        }
    }

    @Test
    void listQueriesWithPagination() throws Exception {
        // Register several queries
        for (int i = 0; i < 5; i++) {
            QueryStateMachine sm = new QueryStateMachine();
            sm.tryTransition(QueryStateMachine.State.PLANNING);
            queryManager.registerStateMachine("status-page-" + i, sm);
        }

        try {
            HttpResponse<String> response = sendGet("/v1/query-status?pageSize=2&pageToken=0");
            assertThat(response.statusCode()).isEqualTo(200);

            Map<String, Object> body = MAPPER.readValue(response.body(), new TypeReference<>() {});
            List<Map<String, Object>> queries = (List<Map<String, Object>>) body.get("queries");
            assertThat(queries.size()).isLessThanOrEqualTo(2);

            int totalCount = (int) body.get("totalCount");
            if (totalCount > 2) {
                assertThat(body.get("nextPageToken")).isNotNull();
            }
        } finally {
            for (int i = 0; i < 5; i++) {
                QueryStateMachine sm = queryManager.getStateMachine("status-page-" + i);
                if (sm != null) {
                    sm.tryTransition(QueryStateMachine.State.FAILED);
                }
            }
        }
    }

    @Test
    void getQueryDetailReturnsDetailedInfo() throws Exception {
        QueryStateMachine sm = new QueryStateMachine();
        sm.setTenantId("detail-user");
        sm.tryTransition(QueryStateMachine.State.PLANNING);
        sm.tryTransition(QueryStateMachine.State.SCHEDULING);
        sm.tryTransition(QueryStateMachine.State.RUNNING);
        // Add stage state
        sm.updateStageState("stage-0", StageStateMachine.State.FINISHED);
        sm.updateStageState("stage-1", StageStateMachine.State.RUNNING);
        queryManager.registerStateMachine("status-detail-q1", sm);

        try {
            HttpResponse<String> response = sendGet("/v1/query-status/status-detail-q1");
            assertThat(response.statusCode()).isEqualTo(200);

            Map<String, Object> body = MAPPER.readValue(response.body(), new TypeReference<>() {});
            assertThat(body.get("queryId")).isEqualTo("status-detail-q1");
            assertThat(body.get("state")).isEqualTo("RUNNING");
            assertThat(body.get("tenantId")).isEqualTo("detail-user");
            assertThat(body.get("terminal")).isEqualTo(false);
            assertThat(body.get("stageCount")).isEqualTo(2);
            assertThat(body.get("createdAt")).isNotNull();
            assertThat((int) body.get("elapsedMs")).isGreaterThanOrEqualTo(0);
            assertThat(body.get("errorCode")).isNull();
            assertThat(body.get("errorMessage")).isNull();
        } finally {
            sm.tryTransition(QueryStateMachine.State.FINISHING);
            sm.tryTransition(QueryStateMachine.State.FINISHED);
        }
    }

    @Test
    void getQueryDetailReturnsErrorInfoForFailedQuery() throws Exception {
        QueryStateMachine sm = new QueryStateMachine();
        sm.tryTransition(QueryStateMachine.State.PLANNING);
        sm.tryTransition(QueryStateMachine.State.SCHEDULING);
        sm.tryTransition(QueryStateMachine.State.RUNNING);
        sm.setError(new ErrorDetail("INTERNAL_ERROR", "E001", "Something went wrong"));
        sm.tryTransition(QueryStateMachine.State.FAILED);
        queryManager.registerStateMachine("status-failed-q1", sm);

        HttpResponse<String> response = sendGet("/v1/query-status/status-failed-q1");
        assertThat(response.statusCode()).isEqualTo(200);

        Map<String, Object> body = MAPPER.readValue(response.body(), new TypeReference<>() {});
        assertThat(body.get("queryId")).isEqualTo("status-failed-q1");
        assertThat(body.get("state")).isEqualTo("FAILED");
        assertThat(body.get("terminal")).isEqualTo(true);
        // ErrorDetail fields: errorCode and errorMessage come from the QueryDetail record
        assertThat(body.get("errorCode")).isEqualTo("INTERNAL_ERROR");
        assertThat(body.get("errorMessage")).isEqualTo("Something went wrong");
    }

    @Test
    void getQueryDetailReturns404ForUnknownQuery() throws Exception {
        HttpResponse<String> response = sendGet("/v1/query-status/nonexistent-query");
        assertThat(response.statusCode()).isEqualTo(404);
    }

    @Test
    void getQueryStagesReturnsStageBreakdown() throws Exception {
        QueryStateMachine sm = new QueryStateMachine();
        sm.tryTransition(QueryStateMachine.State.PLANNING);
        sm.tryTransition(QueryStateMachine.State.SCHEDULING);
        sm.tryTransition(QueryStateMachine.State.RUNNING);
        sm.updateStageState("stage-0", StageStateMachine.State.FINISHED);
        sm.updateStageState("stage-1", StageStateMachine.State.RUNNING);
        sm.updateStageState("stage-2", StageStateMachine.State.SCHEDULING);
        queryManager.registerStateMachine("status-stages-q1", sm);

        try {
            HttpResponse<String> response = sendGet("/v1/query-status/status-stages-q1/stages");
            assertThat(response.statusCode()).isEqualTo(200);

            Map<String, Object> body = MAPPER.readValue(response.body(), new TypeReference<>() {});
            assertThat(body.get("queryId")).isEqualTo("status-stages-q1");
            assertThat(body.get("queryState")).isEqualTo("RUNNING");

            List<Map<String, Object>> stages = (List<Map<String, Object>>) body.get("stages");
            assertThat(stages).hasSize(3);

            // Stages should be sorted by stageId
            Map<String, Object> s0 = stages.stream()
                    .filter(s -> "stage-0".equals(s.get("stageId")))
                    .findFirst().orElseThrow();
            assertThat(s0.get("state")).isEqualTo("FINISHED");

            Map<String, Object> s1 = stages.stream()
                    .filter(s -> "stage-1".equals(s.get("stageId")))
                    .findFirst().orElseThrow();
            assertThat(s1.get("state")).isEqualTo("RUNNING");

            Map<String, Object> s2 = stages.stream()
                    .filter(s -> "stage-2".equals(s.get("stageId")))
                    .findFirst().orElseThrow();
            assertThat(s2.get("state")).isEqualTo("SCHEDULING");
        } finally {
            sm.tryTransition(QueryStateMachine.State.FINISHING);
            sm.tryTransition(QueryStateMachine.State.FINISHED);
        }
    }

    @Test
    void getQueryStagesReturns404ForUnknownQuery() throws Exception {
        HttpResponse<String> response = sendGet("/v1/query-status/nonexistent/stages");
        assertThat(response.statusCode()).isEqualTo(404);
    }

    @Test
    void getQueryStagesReturnsEmptyWhenNoStages() throws Exception {
        QueryStateMachine sm = new QueryStateMachine();
        sm.tryTransition(QueryStateMachine.State.PLANNING);
        queryManager.registerStateMachine("status-no-stages-q1", sm);

        try {
            HttpResponse<String> response = sendGet("/v1/query-status/status-no-stages-q1/stages");
            assertThat(response.statusCode()).isEqualTo(200);

            Map<String, Object> body = MAPPER.readValue(response.body(), new TypeReference<>() {});
            assertThat(body.get("queryId")).isEqualTo("status-no-stages-q1");
            List<Map<String, Object>> stages = (List<Map<String, Object>>) body.get("stages");
            assertThat(stages).isEmpty();
        } finally {
            sm.tryTransition(QueryStateMachine.State.FAILED);
        }
    }

    private static HttpResponse<String> sendGet(String path) throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + path))
                .GET()
                .build();
        return HTTP_CLIENT.send(request, HttpResponse.BodyHandlers.ofString());
    }
}

package org.pangrova.coordinator.http.endpoint;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.pangrova.common.http.adapter.jetty.EmbeddedHttpServerFactory;
import org.pangrova.common.json.provider.jackson.JsonMapperFactory;
import org.pangrova.coordinator.query.QueryManager;
import org.pangrova.coordinator.query.QueryStateMachine;
import org.pangrova.scheduler.api.placement.HealthState;
import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.common.value.MemoryPressure;
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
import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Integration test for Web UI REST endpoints.
 * Starts a real embedded Jetty + Jersey server and verifies query list and worker
 * status APIs return correct JSON responses.
 */
class WebUiResourceIntegrationTest {

    private static Server server;
    private static int port;
    private static final HttpClient HTTP_CLIENT = HttpClient.newHttpClient();
    private static final ObjectMapper MAPPER = JsonMapperFactory.create();
    private static QueryManager queryManager;
    private static WorkerManager workerManager;

    @BeforeAll
    static void startServer() throws Exception {
        workerManager = new WorkerManager(20_000, 60_000, new org.pangrova.coordination.inmemory.InMemoryWorkerRegistry());
        queryManager = org.pangrova.coordinator.testinfra.StatusOnlyQueryManagerFixture.create(
                workerManager, new org.pangrova.coordinator.task.CoordinatorTaskTracker());

        ResourceConfig config = new ResourceConfig();
        config.register(new WebUiResource(queryManager, workerManager, null));
        config.register(new WebUiStaticResource());
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
    void getQueriesReturnsEmptyListInitially() throws Exception {
        HttpResponse<String> response = sendGet("/api/queries");

        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.headers().firstValue("content-type").orElse(""))
                .contains("application/json");

        List<Map<String, Object>> queries = MAPPER.readValue(
                response.body(), new TypeReference<>() {});
        assertThat(queries).isEmpty();
    }

    @Test
    void getQueriesReturnsRegisteredQuery() throws Exception {
        String queryId = "test-query-001";
        QueryStateMachine sm = new QueryStateMachine();
        sm.setTenantId("test-user");
        sm.tryTransition(QueryStateMachine.State.PLANNING);
        sm.tryTransition(QueryStateMachine.State.SCHEDULING);
        sm.tryTransition(QueryStateMachine.State.RUNNING);
        queryManager.registerStateMachine(queryId, sm);

        try {
            HttpResponse<String> response = sendGet("/api/queries");

            assertThat(response.statusCode()).isEqualTo(200);
            List<Map<String, Object>> queries = MAPPER.readValue(
                    response.body(), new TypeReference<>() {});
            assertThat(queries).isNotEmpty();

            Map<String, Object> query = queries.stream()
                    .filter(q -> queryId.equals(q.get("queryId")))
                    .findFirst()
                    .orElseThrow();
            assertThat(query.get("status")).isEqualTo("RUNNING");
            assertThat(query.get("user")).isEqualTo("test-user");
            assertThat(query.get("durationMs")).isNotNull();
            assertThat(query.get("createdAt")).isNotNull();
        } finally {
            sm.tryTransition(QueryStateMachine.State.FINISHING);
            sm.tryTransition(QueryStateMachine.State.FINISHED);
        }
    }

    @Test
    void getWorkersReturnsEmptyListInitially() throws Exception {
        HttpResponse<String> response = sendGet("/api/workers");

        assertThat(response.statusCode()).isEqualTo(200);
        List<Map<String, Object>> workers = MAPPER.readValue(
                response.body(), new TypeReference<>() {});
        assertThat(workers).isEmpty();
    }

    @Test
    void getWorkersReturnsRegisteredWorker() throws Exception {
        WorkerInfo worker = new WorkerInfo(
                "worker-001", "localhost", 8080, 9090, 9091,
                HealthState.ACTIVE, MemoryPressure.NONE,
                0.5, 3, 1024L * 1024 * 512,
                Instant.now(), 0.9, null, 2, null, List.of(), null,
                org.pangrova.coordinator.worker.TestWorkerInfoBuilder.canonicalInventory(),
                org.pangrova.coordinator.worker.TestWorkerInfoBuilder.executionIdentity("worker-001"));
        workerManager.registerWorker(worker);

        try {
            HttpResponse<String> response = sendGet("/api/workers");

            assertThat(response.statusCode()).isEqualTo(200);
            List<Map<String, Object>> workers = MAPPER.readValue(
                    response.body(), new TypeReference<>() {});
            assertThat(workers).hasSize(1);

            Map<String, Object> w = workers.get(0);
            assertThat(w.get("workerId")).isEqualTo("worker-001");
            assertThat(w.get("host")).isEqualTo("localhost");
            assertThat(w.get("healthState")).isEqualTo("ACTIVE");
            assertThat(w.get("memoryPressure")).isEqualTo("NONE");
            assertThat(w.get("runningTaskCount")).isEqualTo(3);
        } finally {
            workerManager.removeWorker("worker-001");
        }
    }

    @Test
    void getQueryProfileReturnsServiceUnavailableWhenNoProfileService() throws Exception {
        HttpResponse<String> response = sendGet("/api/queries/nonexistent/profile");

        assertThat(response.statusCode()).isEqualTo(503);
    }

    @Test
    void staticUiIndexReturnsHtml() throws Exception {
        HttpResponse<String> response = sendGet("/ui");

        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.body()).contains("Pangrova Dashboard");
        assertThat(response.headers().firstValue("content-type").orElse(""))
                .contains("text/html");
    }

    @Test
    void staticUiCssReturnsStylesheet() throws Exception {
        HttpResponse<String> response = sendGet("/ui/style.css");

        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.body()).contains("body");
        assertThat(response.headers().firstValue("content-type").orElse(""))
                .contains("text/css");
    }

    @Test
    void staticUiJsReturnsJavascript() throws Exception {
        HttpResponse<String> response = sendGet("/ui/app.js");

        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.body()).contains("loadQueries");
        assertThat(response.headers().firstValue("content-type").orElse(""))
                .contains("javascript");
    }

    @Test
    void staticUiPathTraversalReturns400() throws Exception {
        HttpResponse<String> response = sendGet("/ui/../../../etc/passwd");

        assertThat(response.statusCode()).isIn(400, 404);
    }

    private static HttpResponse<String> sendGet(String path) throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + path))
                .GET()
                .build();
        return HTTP_CLIENT.send(request, HttpResponse.BodyHandlers.ofString());
    }
}

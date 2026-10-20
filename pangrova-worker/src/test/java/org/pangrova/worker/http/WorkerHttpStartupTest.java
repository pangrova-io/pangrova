package org.pangrova.worker.http;

import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.pangrova.common.http.HttpClientFactory;
import org.pangrova.common.http.ManagedHttpClient;
import org.pangrova.testing.TestExchangeTransport;
import org.pangrova.worker.task.TaskManager;

import static org.assertj.core.api.Assertions.assertThat;

/** First-request routing and authentication after cold startup; task execution has separate JNI tests. */
class WorkerHttpStartupTest {
    private static final String SECRET = "startup-test-service-secret";
    private TaskManager tasks;
    private WorkerHttpServer server;
    private ManagedHttpClient http;

    @BeforeEach
    void startWithoutAnyWarmupRequest() throws Exception {
        tasks = TaskManager.forHttpWiringOnly();
        server = new WorkerHttpServer(new WorkerHttpServer.WorkerHttpConfig(0, "127.0.0.1", 0, SECRET),
                tasks, TestExchangeTransport.INSTANCE);
        server.start();
        http = HttpClientFactory.createDefault();
    }

    @AfterEach
    void releaseServerAndOwners() throws Exception {
        try {
            if (server != null) server.stop();
        } finally {
            if (tasks != null) tasks.close();
            if (http != null) http.close();
        }
    }

    @RepeatedTest(10)
    void firstRequestAfterStartResolvesTheTaskRouteWithoutRetry() throws Exception {
        var response = http.httpClient().send(HttpRequest.newBuilder(taskUri())
                .timeout(Duration.ofSeconds(5)).header("X-Internal-Auth", SECRET).GET().build(),
                HttpResponse.BodyHandlers.discarding());
        assertThat(response.statusCode()).isEqualTo(405);
        assertThat(response.headers().firstValue("Allow"))
                .hasValueSatisfying(value -> assertThat(value).contains("POST"));
    }

    @Test
    void authenticationRejectsTheFirstUnauthenticatedSubmission() throws Exception {
        var response = http.httpClient().send(HttpRequest.newBuilder(taskUri())
                .timeout(Duration.ofSeconds(5)).header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString("{}")).build(), HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(401);
        assertThat(response.body()).contains("INTERNAL_AUTH");
    }

    private URI taskUri() {
        return URI.create("http://127.0.0.1:" + server.getPort() + "/v1/task");
    }
}

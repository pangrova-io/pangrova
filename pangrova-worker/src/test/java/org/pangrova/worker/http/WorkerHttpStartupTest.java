package org.pangrova.worker.http;

import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.pangrova.transport.http.client.HttpClientFactory;
import org.pangrova.transport.http.client.ManagedHttpClient;
import org.pangrova.testing.testkit.TestExchangeTransport;
import org.pangrova.worker.task.TaskManager;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** First-request routing and authentication after cold startup; task execution has separate JNI tests. */
class WorkerHttpStartupTest {
    private static final String SECRET = "startup-test-service-secret";
    private TaskManager tasks;
    private WorkerHttpServer server;
    private ManagedHttpClient http;

    @BeforeEach
    void startWithoutAnyWarmupRequest() throws Exception {
        tasks = TaskManager.forHttpWiringOnly();
        server = new WorkerHttpServer(new WorkerHttpServer.WorkerHttpConfig(0, "127.0.0.1", 0, SECRET)
                        .withBindHost("127.0.0.1"),
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

    @Test
    void stopAlsoClosesTheFileEndpointFactory() throws Exception {
        var factory = org.pangrova.worker.file.ObjectStoreAccessorFactory.discover();
        var files = new org.pangrova.worker.file.FileSpaceFileResource(factory, SECRET, event -> {});
        var localTasks = TaskManager.forHttpWiringOnly();
        var withFiles = new WorkerHttpServer(
                new WorkerHttpServer.WorkerHttpConfig(0, "127.0.0.1", 0, SECRET)
                        .withBindHost("127.0.0.1"), localTasks,
                TestExchangeTransport.INSTANCE, null, null, null, null, files);
        try {
            withFiles.start();
            withFiles.stop();
            var profile = new org.pangrova.filesystem.api.profile.StorageProfile("closed",
                    org.pangrova.common.enums.StorageType.LOCAL, "/tmp", "", "", "us-east-1",
                    java.util.Map.of(), java.util.Map.of());
            assertThatThrownBy(() -> factory.accessor(profile)).hasMessageContaining("closed");
            files.close();
        } finally {
            withFiles.stop();
            localTasks.close();
        }
    }

    @Test
    void anOccupiedLoopbackListenerCannotBeAdvertisedAsAnotherWorker() throws Exception {
        var otherTasks = TaskManager.forHttpWiringOnly();
        var other = new WorkerHttpServer(new WorkerHttpServer.WorkerHttpConfig(
                server.getPort(), "another-advertised-worker", 0, SECRET)
                .withBindHost("127.0.0.1"), otherTasks, TestExchangeTransport.INSTANCE);
        try {
            assertThatThrownBy(other::start)
                    .hasRootCauseInstanceOf(java.net.BindException.class)
                    .satisfies(failure -> assertThat(failure.getSuppressed()).isEmpty());
        } finally {
            try {
                other.stop();
            } finally {
                otherTasks.close();
            }
        }
    }

    private URI taskUri() {
        return URI.create("http://127.0.0.1:" + server.getPort() + "/v1/task");
    }
}

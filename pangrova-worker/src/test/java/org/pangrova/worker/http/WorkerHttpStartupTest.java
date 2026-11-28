package org.pangrova.worker.http;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.net.Socket;
import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.pangrova.protocol.http.TaskSubmitAck;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
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
                .timeout(Duration.ofSeconds(5)).header("Content-Type", "application/x-protobuf")
                .POST(HttpRequest.BodyPublishers.ofByteArray(new byte[]{(byte) 0x80})).build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(401);
        assertThat(response.body()).contains("INTERNAL_AUTH");
    }

    @Test
    void taskIngressRejectsLegacyJsonAndMalformedProtobuf() throws Exception {
        var json = http.httpClient().send(HttpRequest.newBuilder(taskUri()).timeout(Duration.ofSeconds(5))
                .header("X-Internal-Auth", SECRET).header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString("{\"canonical_task_request\":\"CgA=\"}")).build(),
                HttpResponse.BodyHandlers.discarding());
        assertThat(json.statusCode()).isEqualTo(415);

        var malformed = http.httpClient().send(HttpRequest.newBuilder(taskUri()).timeout(Duration.ofSeconds(5))
                .header("X-Internal-Auth", SECRET).header("Content-Type", "application/x-protobuf")
                .POST(HttpRequest.BodyPublishers.ofInputStream(
                        () -> new ByteArrayInputStream(new byte[]{(byte) 0x80}))).build(),
                HttpResponse.BodyHandlers.ofByteArray());
        assertThat(malformed.statusCode()).isEqualTo(400);
        assertThat(malformed.headers().firstValue("Content-Type")).contains("application/x-protobuf");
        var ack = TaskSubmitAck.parseFrom(malformed.body());
        assertThat(ack.hasRejected()).isTrue();
        assertThat(ack.getRejected().getCode()).isEqualTo("BAD_REQUEST");
    }

    @Test
    void retiredUpstreamNotificationCannotBePosted() throws Exception {
        var response = http.httpClient().send(HttpRequest.newBuilder(URI.create(taskUri() + "/upstream-commit"))
                .timeout(Duration.ofSeconds(5)).header("X-Internal-Auth", SECRET)
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString("{\"queryId\":\"query\",\"upstreamStageId\":0}")).build(),
                HttpResponse.BodyHandlers.discarding());
        assertThat(response.statusCode()).isEqualTo(405);
        assertThat(response.headers().firstValue("Allow"))
                .hasValueSatisfying(value -> assertThat(value).contains("DELETE").doesNotContain("POST"));
    }

    @Test
    void oversizedContentLengthIsRejectedBeforeReadingTheBody() throws Exception {
        try (var socket = new Socket(taskUri().getHost(), taskUri().getPort())) {
            socket.setSoTimeout(5_000);
            var request = "POST /v1/task HTTP/1.1\r\n"
                    + "Host: " + taskUri().getAuthority() + "\r\n"
                    + "X-Internal-Auth: " + SECRET + "\r\n"
                    + "Content-Type: " + SubmitTaskRequest.MEDIA_TYPE + "\r\n"
                    + "Content-Length: " + (SubmitTaskRequest.MAX_HTTP_BODY_BYTES + 1L) + "\r\n"
                    + "Connection: close\r\n\r\n";
            socket.getOutputStream().write(request.getBytes(StandardCharsets.US_ASCII));
            // Send one byte so the router can identify a present entity, then wait for rejection.
            socket.getOutputStream().write(0x0a);
            socket.getOutputStream().flush();
            var response = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.US_ASCII));
            assertThat(response.readLine()).startsWith("HTTP/1.1 413 ");
        }
    }

    @Test
    void requestByteCapacityCoversDeclaredAndStreamingBodiesBeforeDecoding() throws Exception {
        byte[] malformed = {(byte) 0x80};
        var declared = HttpRequest.BodyPublishers.ofByteArray(malformed);
        var chunked = HttpRequest.BodyPublishers.ofInputStream(() -> new ByteArrayInputStream(malformed));
        try (var admitted = tasks.admitTaskBody()) {
            admitted.reserve(SubmitTaskRequest.MAX_HTTP_BODY_BYTES);
            for (var sender : java.util.List.of(declared, chunked)) {
                var response = http.httpClient().send(HttpRequest.newBuilder(taskUri())
                        .timeout(Duration.ofSeconds(5)).header("X-Internal-Auth", SECRET)
                        .header("Content-Type", SubmitTaskRequest.MEDIA_TYPE).POST(sender).build(),
                        HttpResponse.BodyHandlers.ofByteArray());
                assertThat(response.statusCode()).isEqualTo(503);
                var ack = TaskSubmitAck.parseFrom(response.body());
                assertThat(ack.getRejected().getCode()).isEqualTo("SERVICE_UNAVAILABLE");
                assertThat(ack.getRejected().getReason()).contains("task submission byte capacity");
            }
        }
        var recovered = http.httpClient().send(HttpRequest.newBuilder(taskUri())
                .timeout(Duration.ofSeconds(5)).header("X-Internal-Auth", SECRET)
                .header("Content-Type", SubmitTaskRequest.MEDIA_TYPE).POST(declared).build(),
                HttpResponse.BodyHandlers.ofByteArray());
        assertThat(recovered.statusCode()).isEqualTo(400);
        assertThat(TaskSubmitAck.parseFrom(recovered.body()).getRejected().getReason())
                .contains("Invalid binary task");
        try (var available = tasks.admitTaskBody()) {
            available.reserve(SubmitTaskRequest.MAX_HTTP_BODY_BYTES);
        }
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

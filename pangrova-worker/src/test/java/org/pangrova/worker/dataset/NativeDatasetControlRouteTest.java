package org.pangrova.worker.dataset;

import com.google.protobuf.Any;
import org.junit.jupiter.api.Test;
import org.pangrova.protocol.CanonicalTaskRequests;
import org.pangrova.protocol.v2.FinalizeDistributedWriteRequest;
import org.pangrova.protocol.v2.RuntimeCommand;
import org.pangrova.protocol.v2.RuntimeCommandResponse;
import org.pangrova.protocol.v2.RuntimeCommandType;
import org.pangrova.worker.exchange.NativeExchangeTransport;
import org.pangrova.worker.execution.NativeTaskExecutor;
import org.pangrova.worker.http.WorkerHttpServer;
import org.pangrova.worker.task.TaskManager;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/** Real Jetty/Jersey registration, shared authentication and fail-closed binary admission. */
class NativeDatasetControlRouteTest {
    @Test
    void authenticatesAndReturnsCanonicalBinaryRejectionsOnTheRegisteredRoute() throws Exception {
        var fetches = new AtomicInteger();
        try (var tasks = TaskManager.forHttpWiringOnly(); var catalogs = NativeDatasetControlServiceTest.catalogs(fetches);
                var executor = new NativeTaskExecutor(); var exchange = new NativeExchangeTransport(executor)) {
            tasks.requireControlEpochHandshake();
            tasks.observeControlEpoch(2);
            var control = new NativeDatasetControlService(tasks, catalogs);
            var server = new WorkerHttpServer(new WorkerHttpServer.WorkerHttpConfig(
                    0, "localhost", 0, "control-route-test-secret", "grpc"), tasks, exchange, null, control);
            try {
                server.start();
                URI endpoint = URI.create("http://127.0.0.1:" + server.getPort() + "/v1/native-dataset/control");
                var client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(30)).build();
                assertThat(post(client, endpoint, "wrong-secret", new byte[] {1}).statusCode()).isEqualTo(401);
                var malformed = post(client, endpoint, "control-route-test-secret", new byte[] {(byte) 0xff});
                assertThat(malformed.statusCode()).isEqualTo(400);
                assertThat(malformed.headers().firstValue("Content-Type").orElseThrow())
                        .startsWith("application/octet-stream");
                assertThat(RuntimeCommandResponse.parseFrom(malformed.body()).getError().getCode())
                        .isEqualTo("INVALID_METADATA_CONTROL");
                var denied = post(client, endpoint, "control-route-test-secret",
                        NativeDatasetControlServiceTest.schemaCommand("denied-tenant").toByteArray());
                assertThat(denied.statusCode()).isEqualTo(403);
                assertThat(RuntimeCommandResponse.parseFrom(denied.body()).getError().getCode())
                        .isEqualTo("CATALOG_TENANT_DENIED");
                var stale = post(client, endpoint, "control-route-test-secret", staleFinalize().toByteArray());
                assertThat(stale.statusCode()).isEqualTo(409);
                assertThat(RuntimeCommandResponse.parseFrom(stale.body()).getError().getCode())
                        .isEqualTo("EPOCH_MISMATCH");
                assertThat(fetches).hasValue(1);
            } finally {
                server.stop();
            }
        }
    }

    private static RuntimeCommand staleFinalize() {
        return RuntimeCommand.newBuilder().setProtocol(CanonicalTaskRequests.protocolVersion())
                .setCommandType(RuntimeCommandType.RUNTIME_COMMAND_TYPE_FINALIZE_DISTRIBUTED_WRITE)
                .setPayload(Any.pack(FinalizeDistributedWriteRequest.newBuilder().setControlEpoch(1).build())).build();
    }

    private static HttpResponse<byte[]> post(HttpClient client, URI endpoint, String auth, byte[] bytes)
            throws Exception {
        var request = HttpRequest.newBuilder(endpoint).timeout(Duration.ofSeconds(30))
                .header("X-Internal-Auth", auth).header("Content-Type", "application/octet-stream")
                .header("Accept", "application/octet-stream").POST(HttpRequest.BodyPublishers.ofByteArray(bytes)).build();
        return client.send(request, HttpResponse.BodyHandlers.ofByteArray());
    }
}

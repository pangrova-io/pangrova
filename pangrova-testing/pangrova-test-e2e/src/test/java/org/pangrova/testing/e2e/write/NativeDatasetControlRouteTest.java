package org.pangrova.testing.e2e.write;

import com.google.protobuf.Any;
import org.junit.jupiter.api.Test;
import org.pangrova.protocol.task.CanonicalTaskRequests;
import org.pangrova.protocol.v2.PrepareDistributedWriteRequest;
import org.pangrova.protocol.v2.DatasetSchemaFingerprintRequest;
import org.pangrova.protocol.v2.DatasetSchemaFingerprintResult;
import org.pangrova.protocol.v2.Checksum;
import org.pangrova.protocol.v2.RuntimeCommand;
import org.pangrova.protocol.v2.RuntimeCommandResponse;
import org.pangrova.protocol.v2.RuntimeCommandType;
import org.pangrova.testing.E2ETest;
import org.pangrova.worker.exchange.NativeExchangeTransport;
import org.pangrova.worker.execution.NativeTaskExecutor;
import org.pangrova.worker.http.WorkerHttpServer;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/** Real Jetty/Jersey registration, shared authentication and fail-closed binary admission. */
class NativeDatasetControlRouteTest extends DatasetControlTestBase {
    @Test
    void authenticatesAndReturnsCanonicalBinaryRejectionsOnTheRegisteredRoute() throws Exception {
        try (var fixture = controlFixture();
                var executor = new NativeTaskExecutor(); var exchange = new NativeExchangeTransport(executor)) {
            var tasks = fixture.tasks();
            tasks.requireControlEpochHandshake();
            tasks.observeControlEpoch(2);
            var control = fixture.control();
            var server = new WorkerHttpServer(new WorkerHttpServer.WorkerHttpConfig(
                    0, "localhost", 0, E2E_INTERNAL_AUTH_SECRET, "grpc"), tasks, exchange, null, control);
            try {
                server.start();
                URI endpoint = URI.create("http://127.0.0.1:" + server.getPort() + "/v1/native-dataset/control");
                var client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(30)).build();
                assertThat(post(client, endpoint, "wrong-secret", new byte[] {1}).statusCode()).isEqualTo(401);
                var malformed = post(client, endpoint, E2E_INTERNAL_AUTH_SECRET, new byte[] {(byte) 0xff});
                assertThat(malformed.statusCode()).isEqualTo(400);
                assertThat(malformed.headers().firstValue("Content-Type").orElseThrow())
                        .startsWith("application/octet-stream");
                assertThat(RuntimeCommandResponse.parseFrom(malformed.body()).getError().getCode())
                        .isEqualTo("INVALID_METADATA_CONTROL");
                var denied = post(client, endpoint, E2E_INTERNAL_AUTH_SECRET, schemaCommand(true).toByteArray());
                assertThat(denied.statusCode()).isEqualTo(403);
                assertThat(RuntimeCommandResponse.parseFrom(denied.body()).getError().getCode())
                        .isEqualTo("CATALOG_TENANT_DENIED");
                var stale = post(client, endpoint, E2E_INTERNAL_AUTH_SECRET, stalePrepare().toByteArray());
                assertThat(stale.statusCode()).isEqualTo(409);
                assertThat(RuntimeCommandResponse.parseFrom(stale.body()).getError().getCode())
                        .isEqualTo("EPOCH_MISMATCH");
                assertThat(fixture.fetchCount()).isEqualTo(1);
            } finally {
                server.stop();
            }
        }
    }

    @Test
    @E2ETest
    void authenticatedFingerprintUsesRealNativeAndCatalogAlsoSupportsPublicSqlRoundTrip() throws Exception {
        try (var fixture = controlFixture();
                var executor = new NativeTaskExecutor(); var exchange = new NativeExchangeTransport(executor)) {
            var server = new WorkerHttpServer(new WorkerHttpServer.WorkerHttpConfig(
                    0, "localhost", 0, E2E_INTERNAL_AUTH_SECRET, "grpc"),
                    fixture.tasks(), exchange, null, fixture.control());
            try {
                server.start();
                URI endpoint = URI.create("http://127.0.0.1:" + server.getPort() + "/v1/native-dataset/control");
                var client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(30)).build();
                var command = schemaCommand(false);
                var first = fingerprint(post(client, endpoint, E2E_INTERNAL_AUTH_SECRET, command.toByteArray()));
                var second = fingerprint(post(client, endpoint, E2E_INTERNAL_AUTH_SECRET, command.toByteArray()));
                assertThat(first.getPhysicalSchemaSha256().getAlgorithm())
                        .isEqualTo(Checksum.Algorithm.ALGORITHM_SHA256);
                assertThat(first.getPhysicalSchemaSha256().getDigest().size()).isEqualTo(32);
                assertThat(second).isEqualTo(first);
                var request = command.getPayload().unpack(DatasetSchemaFingerprintRequest.class);
                var renamed = command.toBuilder().setPayload(Any.pack(request.toBuilder()
                        .setSchema(request.getSchema().toBuilder().setNames(0, "renamed_record")).build())).build();
                var changed = fingerprint(post(client, endpoint, E2E_INTERNAL_AUTH_SECRET, renamed.toByteArray()));
                assertThat(changed.getPhysicalSchemaSha256()).isNotEqualTo(first.getPhysicalSchemaSha256());
                assertThat(fixture.fetchCount()).isEqualTo(1);
                assertThat(fixture.catalogs().cachedCatalogRevisionCount()).isEqualTo(1);

                String table = DATASET + "." + SCHEMA + ".control_"
                        + java.util.UUID.randomUUID().toString().replace("-", "");
                sqlRunner.executeDdl("CREATE TABLE " + table + " (id BIGINT)");
                var submitted = submitQueryViaHttp("INSERT INTO " + table + " VALUES (7), (9)");
                assertThat(pollUntilTerminal(submitted.queryId())).contains("\"FINISHED\"");
                var selected = framework.executeQuery("SELECT id FROM " + table + " ORDER BY id");
                assertThat(selected.success()).as("actual SQL read: %s", selected.errorMessage()).isTrue();
                assertThat(selected.rows()).extracting(row -> row.getLong("id")).containsExactly(7L, 9L);
            } finally {
                server.stop();
            }
        }
    }

    private static DatasetSchemaFingerprintResult fingerprint(HttpResponse<byte[]> response) throws Exception {
        assertThat(response.statusCode()).isEqualTo(200);
        var result = RuntimeCommandResponse.parseFrom(response.body());
        assertThat(result.getSuccess()).as("actual Native response: %s", result.getError().getMessage()).isTrue();
        return result.getPayload().unpack(DatasetSchemaFingerprintResult.class);
    }

    private static RuntimeCommand stalePrepare() {
        return RuntimeCommand.newBuilder().setProtocol(CanonicalTaskRequests.protocolVersion())
                .setCommandType(RuntimeCommandType.RUNTIME_COMMAND_TYPE_PREPARE_DISTRIBUTED_WRITE)
                .setPayload(Any.pack(PrepareDistributedWriteRequest.newBuilder().setControlEpoch(1).build())).build();
    }

    private static HttpResponse<byte[]> post(HttpClient client, URI endpoint, String auth, byte[] bytes)
            throws Exception {
        var request = HttpRequest.newBuilder(endpoint).timeout(Duration.ofSeconds(30))
                .header("X-Internal-Auth", auth).header("Content-Type", "application/octet-stream")
                .header("Accept", "application/octet-stream").POST(HttpRequest.BodyPublishers.ofByteArray(bytes)).build();
        return client.send(request, HttpResponse.BodyHandlers.ofByteArray());
    }
}

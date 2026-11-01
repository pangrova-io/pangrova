package org.pangrova.coordinator.file;

import org.pangrova.spi.file.access.FileAccessGrant;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.common.json.provider.jackson.JsonMapperFactory;
import org.pangrova.filesystem.api.object.FileListing;
import org.pangrova.spi.file.access.FileUrlClient;
import org.pangrova.file.adapter.worker.WorkerFileClient;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WorkerFileClientTest {

    private static final String SECRET = "worker-file-client-secret";

    private final ObjectMapper mapper = JsonMapperFactory.create();
    private HttpServer server;
    private volatile boolean fail;
    private volatile String presignResponse = "{\"url\":\"https://storage.example/a.jpg?sig=x\","
            + "\"expiresAtEpochMillis\":1234}";
    private volatile Map<String, Object> received;
    private volatile String receivedAuth;
    private volatile String receivedMethod;
    private volatile String receivedContentType;
    private volatile String receivedQuery;

    @BeforeEach
    void setUp() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/v1/file-space/list", exchange -> {
            receivedAuth = exchange.getRequestHeaders().getFirst("X-Internal-Auth");
            received = mapper.readValue(exchange.getRequestBody().readAllBytes(), Map.class);
            int status = fail ? 503 : 200;
            byte[] body = (fail ? "{}" : listingJson()).getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(status, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        server.createContext("/v1/file-space/object", exchange -> {
            receivedQuery = exchange.getRequestURI().getRawQuery();
            receivedMethod = exchange.getRequestMethod();
            receivedContentType = exchange.getRequestHeaders().getFirst("Content-Type");
            receivedAuth = exchange.getRequestHeaders().getFirst("X-Internal-Auth");
            received = mapper.readValue(exchange.getRequestBody().readAllBytes(), Map.class);
            int status = fail ? 503 : 204;
            exchange.sendResponseHeaders(status, -1);
            exchange.close();
        });
        server.createContext("/v1/file-space/presign", exchange -> {
            receivedAuth = exchange.getRequestHeaders().getFirst("X-Internal-Auth");
            received = mapper.readValue(exchange.getRequestBody().readAllBytes(), Map.class);
            int status = fail ? 503 : 200;
            byte[] body = (fail ? "{}" : presignResponse).getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(status, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        server.start();
    }

    @AfterEach
    void tearDown() {
        server.stop(0);
    }

    @Test
    void postsTheSignedGrantAndParsesTheListing() {
        FileListing listing = client().list(grant(), "page-2", 250);

        assertThat(listing.files()).singleElement().satisfies(file -> {
            assertThat(file.path()).isEqualTo("images/a.jpg");
            assertThat(file.size()).isEqualTo(10);
            assertThat(file.md5()).isNull();
        });
        assertThat(receivedAuth).isEqualTo(SECRET);
        assertThat(received).containsEntry("descriptor", "descriptor-value")
                .containsEntry("signature", "signature-value")
                .containsEntry("pageToken", "page-2")
                .containsEntry("pageSize", 250);
    }

    @Test
    void deletesWithTheVersionPreconditionInTheBody() {
        client().delete(grant(), "v1");

        assertThat(receivedMethod).isEqualTo("DELETE");
        assertThat(receivedQuery).isEqualTo("descriptor=descriptor-value&signature=signature-value");
        assertThat(receivedContentType).isEqualTo("application/json");
        assertThat(receivedAuth).isEqualTo(SECRET);
        assertThat(received).containsEntry("expectedVersion", "v1").hasSize(1);
    }

    @Test
    void deleteFailureSurfacesAsStorageUnavailable() {
        fail = true;

        assertThatThrownBy(() -> client().delete(grant(), null))
                .isInstanceOfSatisfying(PangrovaException.class, failure ->
                        assertThat(failure.getErrorCode())
                                .isEqualTo(PangrovaErrorCode.FILE_STORAGE_UNAVAILABLE));
    }

    @Test
    void presignExchangesTheGrantForAProviderUrl() {
        FileUrlClient.FileUrl url = client().presign(grant(), true, 600);

        assertThat(url.url()).isEqualTo("https://storage.example/a.jpg?sig=x");
        assertThat(url.expiresAtEpochMillis()).isEqualTo(1234);
        assertThat(receivedAuth).isEqualTo(SECRET);
        assertThat(received).containsEntry("descriptor", "descriptor-value")
                .containsEntry("signature", "signature-value")
                .containsEntry("read", true)
                .containsEntry("ttlSeconds", 600);
    }

    @Test
    void presignFailureSurfacesAsStorageUnavailable() {
        fail = true;

        assertThatThrownBy(() -> client().presign(grant(), false, 600))
                .isInstanceOfSatisfying(PangrovaException.class, failure ->
                        assertThat(failure.getErrorCode())
                                .isEqualTo(PangrovaErrorCode.FILE_STORAGE_UNAVAILABLE));
    }

    @Test
    void invalidReceiptsFailWithoutRetainingCredentialBearingParseDiagnostics() {
        String credential = "secret-provider-signature";
        for (String payload : java.util.List.of("null", "{}", "{\"url\":\"\",\"expiresAtEpochMillis\":1234}",
                "{\"url\":\"https://storage.example/?sig=" + credential + "\",\"expiresAtEpochMillis\":0}",
                "{\"url\":\"https://storage.example/?sig=" + credential
                        + "\",\"expiresAtEpochMillis\":\"" + credential + "\"}")) {
            presignResponse = payload;
            assertThatThrownBy(() -> client().presign(grant(), true, 600))
                    .isInstanceOfSatisfying(PangrovaException.class, failure -> {
                        assertThat(failure.getErrorCode()).isEqualTo(PangrovaErrorCode.FILE_STORAGE_UNAVAILABLE);
                        assertThat(failure.getMessage()).isEqualTo("Worker presign response is invalid");
                        assertThat(failure.getCause()).isNull();
                        var trace = new java.io.StringWriter();
                        failure.printStackTrace(new java.io.PrintWriter(trace));
                        assertThat(trace.toString()).doesNotContain(credential);
                    });
        }
    }

    @Test
    void workerFailureSurfacesAsStorageUnavailable() {
        fail = true;

        assertThatThrownBy(() -> client().list(grant(), null))
                .isInstanceOfSatisfying(PangrovaException.class, failure ->
                        assertThat(failure.getErrorCode())
                                .isEqualTo(PangrovaErrorCode.FILE_STORAGE_UNAVAILABLE));
    }

    private WorkerFileClient client() {
        return new WorkerFileClient(HttpClient.newHttpClient(), SECRET);
    }

    private FileAccessGrant grant() {
        return new FileAccessGrant(
                "http://127.0.0.1:" + server.getAddress().getPort(),
                "descriptor-value", "signature-value", "list", "images", 0L);
    }

    private static String listingJson() {
        return "{\"files\":[{\"path\":\"images/a.jpg\",\"size\":10,"
                + "\"lastModified\":\"2026-10-25T00:00:00Z\",\"etag\":\"etag\","
                + "\"contentType\":\"image/jpeg\",\"providerVersion\":\"v1\",\"md5\":null}],"
                + "\"nextPageToken\":null}";
    }
}

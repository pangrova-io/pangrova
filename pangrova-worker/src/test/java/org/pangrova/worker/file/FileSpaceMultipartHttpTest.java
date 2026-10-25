package org.pangrova.worker.file;

import java.net.ProxySelector;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.common.file.FileAccessSignature;
import org.pangrova.common.http.EmbeddedHttpServerFactory;
import org.pangrova.common.json.JsonMapperFactory;
import org.pangrova.spi.storage.StorageType;
import org.pangrova.spi.storage.file.UploadedPart;
import org.pangrova.worker.http.endpoint.InternalAuthFilter;
import org.pangrova.worker.http.endpoint.WorkerExceptionMapper;

import static org.assertj.core.api.Assertions.assertThat;

/** Multipart grants and content integrity through two real Worker HTTP endpoints. */
class FileSpaceMultipartHttpTest {
    private static final String SECRET = "multipart-http-secret";
    private static final HttpClient CLIENT = HttpClient.newBuilder()
            .proxy(ProxySelector.of(null)).connectTimeout(Duration.ofSeconds(10)).build();
    private final ObjectMapper mapper = JsonMapperFactory.create();
    private final AtomicLong now = new AtomicLong(1_000L);
    @TempDir
    Path root;

    @Test
    void renewsAuthorizationAcrossWorkersAndRejectsStalePartWithoutLosingUpload() throws Exception {
        try (var firstFactory = new ObjectStoreAccessorFactory();
                var secondFactory = new ObjectStoreAccessorFactory()) {
            Server first = server(firstFactory);
            Server second = server(secondFactory);
            try {
                first.start();
                second.start();
                String firstUrl = url(first);
                String secondUrl = url(second);
                String initial = descriptor("write", "clip.bin");
                var session = begin(firstUrl, initial);
                assertThat(session.expiresAtEpochMillis()).isEqualTo(86_401_000L);
                UploadedPart original = part(firstUrl, initial, session.uploadId(), 1, "old");
                now.set(62_000L);
                assertThat(send(partRequest(secondUrl, initial, session.uploadId(), 2, "tail"))
                        .statusCode()).isEqualTo(403);
                String renewed = descriptor("write", "clip.bin");
                var unrelated = begin(firstUrl, descriptor("write", "other.bin"));
                UploadedPart replacement = part(secondUrl, renewed, session.uploadId(), 1, "new");
                UploadedPart tail = part(secondUrl, renewed, session.uploadId(), 2, "tail");
                assertThat(complete(secondUrl, renewed, session.uploadId(), List.of(original, tail))
                        .statusCode()).isEqualTo(409);
                assertThat(complete(secondUrl, renewed, session.uploadId(), List.of(replacement, tail))
                        .statusCode()).isEqualTo(200);
                var read = send(HttpRequest.newBuilder(URI.create(secondUrl + "/object"
                        + query(descriptor("read", "clip.bin")))).GET());
                assertThat(read.statusCode()).isEqualTo(200);
                assertThat(read.body()).isEqualTo("newtail");
                assertThat(send(HttpRequest.newBuilder(URI.create(firstUrl + "/multipart"
                        + query(descriptor("write", "other.bin")) + "&uploadId=" + unrelated.uploadId()))
                        .DELETE()).statusCode()).isEqualTo(204);
            } finally {
                try {
                    second.stop();
                } finally {
                    first.stop();
                }
            }
        }
    }

    private Server server(ObjectStoreAccessorFactory factory) {
        ResourceConfig config = new ResourceConfig()
                .register(new FileSpaceFileResource(factory, SECRET, mapper, now::get))
                .register(new InternalAuthFilter(SECRET)).register(WorkerExceptionMapper.class)
                .register(JacksonFeature.class);
        ServletContextHandler handler = new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
        handler.setContextPath("/");
        handler.addServlet(new ServletHolder(new ServletContainer(config)), "/*");
        return EmbeddedHttpServerFactory.create(0, handler);
    }

    private String url(Server server) {
        return "http://localhost:" + EmbeddedHttpServerFactory.getPort(server) + "/v1/file-space";
    }

    private String descriptor(String operation, String path) throws Exception {
        var grant = new FileAccessDescriptor("local", StorageType.LOCAL, root.toString(),
                "", "", "", false, "stage-root", path, operation, now.get() + 60_000L);
        return Base64.getUrlEncoder().withoutPadding().encodeToString(mapper.writeValueAsBytes(grant));
    }

    private String query(String descriptor) {
        return "?descriptor=" + descriptor + "&signature=" + FileAccessSignature.sign(SECRET, descriptor);
    }

    private FileSpaceFileResource.MultipartSession begin(String base, String descriptor) throws Exception {
        var body = new FileSpaceFileResource.BeginMultipartRequest(descriptor,
                FileAccessSignature.sign(SECRET, descriptor), null);
        var response = send(json(base + "/multipart/begin", body));
        assertThat(response.statusCode()).as(response.body()).isEqualTo(200);
        return mapper.readValue(response.body(), FileSpaceFileResource.MultipartSession.class);
    }

    private HttpRequest.Builder partRequest(String base, String descriptor, String upload,
            int number, String value) {
        return HttpRequest.newBuilder(URI.create(base + "/multipart/part" + query(descriptor)
                        + "&uploadId=" + upload + "&partNumber=" + number))
                .header("Content-Type", "application/octet-stream")
                .PUT(HttpRequest.BodyPublishers.ofString(value));
    }

    private UploadedPart part(String base, String descriptor, String upload, int number,
            String value) throws Exception {
        var response = send(partRequest(base, descriptor, upload, number, value));
        assertThat(response.statusCode()).as(response.body()).isEqualTo(200);
        return mapper.readValue(response.body(), UploadedPart.class);
    }

    private HttpResponse<String> complete(String base, String descriptor, String upload,
            List<UploadedPart> parts) throws Exception {
        var body = new FileSpaceFileResource.CompleteMultipartRequest(descriptor,
                FileAccessSignature.sign(SECRET, descriptor), upload, parts.stream()
                        .map(part -> new FileSpaceFileResource.MultipartPart(part.partNumber(),
                                part.etag(), part.size())).toList());
        return send(json(base + "/multipart/complete", body));
    }

    private HttpRequest.Builder json(String url, Object body) throws Exception {
        return HttpRequest.newBuilder(URI.create(url)).header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(mapper.writeValueAsString(body)));
    }

    private static HttpResponse<String> send(HttpRequest.Builder request) throws Exception {
        return CLIENT.send(request.timeout(Duration.ofSeconds(15)).build(),
                HttpResponse.BodyHandlers.ofString());
    }
}

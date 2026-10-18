package org.pangrova.worker.copy;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.pangrova.common.json.JsonMapperFactory;
import org.pangrova.protocol.rest.copy.CopySourceReadHttp;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Objects;

/** Worker-side client for a task-private Coordinator COPY source-read admission. */
public final class CopySourceReadClient {
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(10);
    private static final int HTTP_OK = 200;
    private static final int MAX_RESPONSE_BYTES = 64 * 1024;

    private final URI baseUri;
    private final String internalAuthSecret;
    private final HttpClient httpClient;
    private final ObjectMapper mapper = JsonMapperFactory.create();

    /** Connects to the Coordinator's internal admission endpoint. */
    public CopySourceReadClient(String coordinatorUrl, String internalAuthSecret) {
        this(coordinatorUrl, internalAuthSecret, false);
    }

    /** Explicit local-only transport for in-process integration tests. */
    public static CopySourceReadClient forLoopbackTest(String coordinatorUrl,
                                                       String internalAuthSecret) {
        return new CopySourceReadClient(coordinatorUrl, internalAuthSecret, true);
    }

    private CopySourceReadClient(String coordinatorUrl, String internalAuthSecret,
                                 boolean allowLoopbackTest) {
        if (coordinatorUrl == null || coordinatorUrl.isBlank()
                || internalAuthSecret == null || internalAuthSecret.isBlank()) {
            throw new IllegalArgumentException("COPY admission client requires Coordinator URL and internal auth");
        }
        baseUri = URI.create(coordinatorUrl.endsWith("/") ? coordinatorUrl : coordinatorUrl + "/");
        if (!"https".equalsIgnoreCase(baseUri.getScheme())
                && !(allowLoopbackTest && "http".equalsIgnoreCase(baseUri.getScheme())
                    && isLoopbackHost(baseUri.getHost()))) {
            throw new IllegalArgumentException(
                    "COPY bearer admission requires HTTPS or a local loopback Coordinator");
        }
        this.internalAuthSecret = internalAuthSecret;
        httpClient = HttpClient.newBuilder().connectTimeout(REQUEST_TIMEOUT).build();
    }

    private static boolean isLoopbackHost(String host) {
        return "127.0.0.1".equals(host) || "[::1]".equals(host)
                || "::1".equals(host);
    }

    /** Revalidates authorization and returns the exact secret-free source profile. */
    public CopySourceReadHttp.RedeemResponse redeem(String nonce, CopySourceReadHttp.Scope scope) {
        Objects.requireNonNull(scope, "scope");
        var response = post("redeem", new CopySourceReadHttp.RedeemRequest(nonce, scope),
                CopySourceReadHttp.RedeemResponse.class);
        var profile = response.profile();
        if (profile == null || !scope.profileName().equals(profile.name())
                || !scope.profileIncarnationId().equals(profile.incarnationId())
                || scope.profileRevision() != profile.revision()
                || !new org.pangrova.spi.storage.StorageProfile(profile.name(), profile.type(),
                        profile.bucket(), profile.prefix(), profile.endpoint(), profile.region(),
                        java.util.Map.of(), java.util.Map.of(
                                "path-style-access", Boolean.toString(profile.pathStyleAccess())))
                        .matchesUrl(scope.canonicalSourceUrl())) {
            throw new IllegalStateException("COPY admission returned a different source profile");
        }
        return response;
    }

    /** Publishes only the exact proof already verified by the Worker stager. */
    public CopySourceReadHttp.StagedSource record(String nonce, CopySourceReadHttp.Scope scope,
                                                  CopySourceReadHttp.StagedSource staged) {
        Objects.requireNonNull(scope, "scope");
        Objects.requireNonNull(staged, "staged");
        var result = post("record", new CopySourceReadHttp.RecordRequest(nonce, scope, staged),
                CopySourceReadHttp.StagedSource.class);
        if (!staged.equals(result)) {
            throw new IllegalStateException("COPY admission returned a different staged object");
        }
        return result;
    }

    private <T> T post(String action, Object body, Class<T> type) {
        URI uri = baseUri.resolve("v1/internal/copy-source-read/" + action);
        try {
            HttpRequest request = HttpRequest.newBuilder(uri).timeout(REQUEST_TIMEOUT)
                    .header("Authorization", "Bearer " + internalAuthSecret)
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofByteArray(mapper.writeValueAsBytes(body)))
                    .build();
            HttpResponse<InputStream> response = httpClient.send(
                    request, HttpResponse.BodyHandlers.ofInputStream());
            try (InputStream input = response.body()) {
                if (response.statusCode() != HTTP_OK) {
                    throw new IllegalStateException(
                            "COPY source-read " + action + " rejected with status " + response.statusCode());
                }
                byte[] bounded = input.readNBytes(MAX_RESPONSE_BYTES + 1);
                if (bounded.length > MAX_RESPONSE_BYTES) {
                    throw new IllegalStateException("COPY source-read response exceeds the size limit");
                }
                return mapper.readValue(bounded, type);
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("COPY source-read " + action + " interrupted", interrupted);
        } catch (IOException failure) {
            throw new IllegalStateException("COPY source-read " + action + " failed", failure);
        }
    }
}

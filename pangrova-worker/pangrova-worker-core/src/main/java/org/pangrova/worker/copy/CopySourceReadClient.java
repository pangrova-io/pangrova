package org.pangrova.worker.copy;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.pangrova.common.json.provider.jackson.JsonMapperFactory;
import org.pangrova.protocol.rest.copy.CopySourceReadHttp;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** Worker-side client for a task-private Coordinator COPY source-read admission. */
public final class CopySourceReadClient {
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(10);
    private static final int HTTP_OK = 200;
    private static final int MAX_RESPONSE_BYTES = 64 * 1024;

    private final URI baseUri;
    private final String internalAuthSecret;
    private final HttpClient httpClient;
    private final Duration requestTimeout;
    private final ObjectMapper mapper = JsonMapperFactory.create();

    /** Connects to the Coordinator's internal admission endpoint. */
    public CopySourceReadClient(String coordinatorUrl, String internalAuthSecret) {
        this(coordinatorUrl, internalAuthSecret, false, REQUEST_TIMEOUT);
    }

    /** Explicit local-only transport for in-process integration tests. */
    public static CopySourceReadClient forLoopbackTest(String coordinatorUrl,
                                                       String internalAuthSecret) {
        return new CopySourceReadClient(coordinatorUrl, internalAuthSecret,
                true, REQUEST_TIMEOUT);
    }

    static CopySourceReadClient forLoopbackTest(String coordinatorUrl,
                                                String internalAuthSecret,
                                                Duration timeout) {
        return new CopySourceReadClient(coordinatorUrl, internalAuthSecret, true, timeout);
    }

    private CopySourceReadClient(String coordinatorUrl, String internalAuthSecret,
                                 boolean allowLoopbackTest, Duration timeout) {
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
        if (timeout == null || timeout.isZero() || timeout.isNegative()) {
            throw new IllegalArgumentException("COPY admission timeout must be positive");
        }
        requestTimeout = timeout;
        this.internalAuthSecret = internalAuthSecret;
        httpClient = HttpClient.newBuilder().connectTimeout(requestTimeout).build();
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
                || !new org.pangrova.filesystem.api.profile.StorageProfile(profile.name(), profile.type(),
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
        var responseBytes = new BoundedResponse();
        try {
            HttpRequest request = HttpRequest.newBuilder(uri).timeout(requestTimeout)
                    .header("Authorization", "Bearer " + internalAuthSecret)
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofByteArray(mapper.writeValueAsBytes(body)))
                    .build();
            // A streaming BodyHandler returns as soon as headers arrive, leaving
            // the later body read outside HttpRequest's timeout. This subscriber
            // completes only after the entire bounded body has arrived.
            var pending = httpClient.sendAsync(request, info -> {
                if (info.statusCode() != HTTP_OK) {
                    return HttpResponse.BodySubscribers.discarding();
                }
                if (info.headers().firstValueAsLong("Content-Length")
                        .orElse(-1) > MAX_RESPONSE_BYTES) {
                    responseBytes.exceeded = true;
                    throw new IllegalStateException("COPY source-read response exceeds the size limit");
                }
                return HttpResponse.BodySubscribers.ofByteArrayConsumer(responseBytes::accept);
            });
            HttpResponse<Void> response;
            try {
                response = pending.get(requestTimeout.toMillis(), TimeUnit.MILLISECONDS);
            } catch (TimeoutException timeout) {
                pending.cancel(true);
                throw new IllegalStateException("COPY source-read " + action + " timed out", timeout);
            } catch (InterruptedException interrupted) {
                pending.cancel(true);
                Thread.currentThread().interrupt();
                throw new IllegalStateException("COPY source-read " + action + " interrupted", interrupted);
            } catch (ExecutionException failure) {
                if (responseBytes.exceeded) {
                    throw new IllegalStateException("COPY source-read response exceeds the size limit",
                            failure.getCause());
                }
                throw new IllegalStateException("COPY source-read " + action + " failed",
                        failure.getCause());
            }
            if (response.statusCode() != HTTP_OK) {
                throw new Rejected(action, response.statusCode());
            }
            return mapper.readValue(responseBytes.bytes(), type);
        } catch (IOException failure) {
            if (responseBytes.exceeded) {
                throw new IllegalStateException("COPY source-read response exceeds the size limit",
                        failure);
            }
            throw new IllegalStateException("COPY source-read " + action + " failed", failure);
        }
    }

    /** Exact control-plane rejection, distinct from a failed or unknown transport exchange. */
    public static final class Rejected extends IllegalStateException {
        private final int status;

        private Rejected(String action, int status) {
            super("COPY source-read " + action + " rejected with status " + status);
            this.status = status;
        }
        public int status() { return status; }
        public boolean isPolicyDenial() {
            return status == java.net.HttpURLConnection.HTTP_UNAUTHORIZED
                    || status == java.net.HttpURLConnection.HTTP_FORBIDDEN;
        }
    }

    private static final class BoundedResponse {
        private final ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        private volatile boolean exceeded;

        private void accept(Optional<byte[]> next) {
            next.ifPresent(chunk -> {
                if (chunk.length > MAX_RESPONSE_BYTES - buffer.size()) {
                    exceeded = true;
                    throw new IllegalStateException("COPY source-read response exceeds the size limit");
                }
                buffer.write(chunk, 0, chunk.length);
            });
        }

        private byte[] bytes() {
            return buffer.toByteArray();
        }
    }
}

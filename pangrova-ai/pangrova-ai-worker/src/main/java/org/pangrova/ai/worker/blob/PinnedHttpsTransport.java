package org.pangrova.ai.worker.blob;

import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.config.ConnectionConfig;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder;
import org.apache.hc.client5.http.protocol.HttpClientContext;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.io.CloseMode;
import org.apache.hc.core5.pool.PoolConcurrencyPolicy;
import org.apache.hc.core5.pool.PoolReusePolicy;
import org.apache.hc.core5.util.TimeValue;
import org.apache.hc.core5.util.Timeout;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Pooled HTTPS transport that connects only to addresses validated by the caller.
 *
 * <p>The target retains the original DNS name for the Host header, TLS SNI and
 * certificate verification while carrying the exact {@link InetAddress} used by
 * the connection manager. This closes the DNS validation-to-connect race without
 * giving up keep-alive or bounded connection pooling.</p>
 */
final class PinnedHttpsTransport
        implements HttpsBlobImporter.HttpsTransport, AutoCloseable {

    private static final String USER_AGENT = "Pangrova-Worker-FILE/1";
    private static final int DEFAULT_HTTPS_PORT = 443;

    private final CloseableHttpClient client;

    PinnedHttpsTransport(HttpsBlobImporter.Settings settings) {
        Objects.requireNonNull(settings, "settings must not be null");
        HttpsBlobImporter.PoolSettings pool = settings.pool();
        PoolingHttpClientConnectionManager connections =
                PoolingHttpClientConnectionManagerBuilder.create()
                        .setPoolConcurrencyPolicy(PoolConcurrencyPolicy.STRICT)
                        .setConnPoolPolicy(PoolReusePolicy.LIFO)
                        .setMaxConnTotal(pool.maxConnections())
                        .setMaxConnPerRoute(pool.maxConnectionsPerRoute())
                        .setConnectionTimeToLive(timeValue(pool.connectionTimeToLive()))
                        .setValidateAfterInactivity(timeValue(pool.idleEviction()))
                        .setDefaultConnectionConfig(ConnectionConfig.custom()
                                .setConnectTimeout(timeout(settings.connectTimeout()))
                                .setSocketTimeout(timeout(settings.readTimeout()))
                                .build())
                        .build();
        this.client = HttpClients.custom()
                .setConnectionManager(connections)
                .disableAutomaticRetries()
                .disableRedirectHandling()
                .disableContentCompression()
                .disableCookieManagement()
                .evictExpiredConnections()
                .evictIdleConnections(timeValue(pool.idleEviction()))
                .setUserAgent(USER_AGENT)
                .build();
    }

    @Override
    public HttpsBlobImporter.DownloadResponse fetch(
            URI uri, List<InetAddress> addresses, Duration readTimeout) throws IOException {
        Objects.requireNonNull(uri, "uri must not be null");
        if (addresses == null || addresses.isEmpty()) {
            throw new IllegalArgumentException("validated HTTPS addresses must not be empty");
        }
        IOException failure = null;
        for (InetAddress address : addresses) {
            try {
                return execute(uri, address, readTimeout);
            } catch (IOException current) {
                if (failure == null) {
                    failure = current;
                } else {
                    failure.addSuppressed(current);
                }
            }
        }
        throw Objects.requireNonNull(failure, "HTTPS connection failure required");
    }

    private HttpsBlobImporter.DownloadResponse execute(
            URI uri, InetAddress address, Duration readTimeout) throws IOException {
        HttpHost target = pinnedTarget(uri, address);
        HttpGet request = new HttpGet(uri);
        request.setHeader("Accept-Encoding", "gzip, identity");
        request.setConfig(RequestConfig.custom()
                .setConnectionRequestTimeout(timeout(readTimeout))
                .setResponseTimeout(timeout(readTimeout))
                .setRedirectsEnabled(false)
                .setAuthenticationEnabled(false)
                .setContentCompressionEnabled(false)
                .build());
        ClassicHttpResponse response = client.executeOpen(
                target, request, HttpClientContext.create());
        try {
            HttpEntity entity = response.getEntity();
            InputStream content = entity == null
                    ? InputStream.nullInputStream() : entity.getContent();
            return new HttpsBlobImporter.DownloadResponse(
                    response.getCode(), headers(response.getHeaders()),
                    new ResponseBody(content, response));
        } catch (RuntimeException | IOException failure) {
            response.close();
            throw failure;
        }
    }

    static HttpHost pinnedTarget(URI uri, InetAddress address) {
        Objects.requireNonNull(uri, "uri must not be null");
        Objects.requireNonNull(address, "address must not be null");
        int port = uri.getPort() == -1 ? DEFAULT_HTTPS_PORT : uri.getPort();
        return new HttpHost("https", address, uri.getHost(), port);
    }

    private static Map<String, List<String>> headers(Header[] headers) {
        Map<String, List<String>> values = new LinkedHashMap<>();
        for (Header header : headers) {
            values.computeIfAbsent(header.getName(), ignored -> new ArrayList<>())
                    .add(header.getValue());
        }
        values.replaceAll((ignored, entries) -> List.copyOf(entries));
        return Map.copyOf(values);
    }

    private static Timeout timeout(Duration duration) {
        return Timeout.ofMilliseconds(duration.toMillis());
    }

    private static TimeValue timeValue(Duration duration) {
        return TimeValue.ofMilliseconds(duration.toMillis());
    }

    @Override
    public void close() {
        client.close(CloseMode.GRACEFUL);
    }

    private static final class ResponseBody extends FilterInputStream {
        private final ClassicHttpResponse response;

        private ResponseBody(InputStream content, ClassicHttpResponse response) {
            super(content);
            this.response = response;
        }

        @Override
        public void close() throws IOException {
            IOException failure = null;
            try {
                super.close();
            } catch (IOException current) {
                failure = current;
            }
            try {
                response.close();
            } catch (IOException current) {
                if (failure == null) {
                    failure = current;
                } else {
                    failure.addSuppressed(current);
                }
            }
            if (failure != null) {
                throw failure;
            }
        }
    }
}

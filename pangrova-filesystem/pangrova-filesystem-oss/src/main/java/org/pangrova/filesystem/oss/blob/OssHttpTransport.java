package org.pangrova.filesystem.oss.blob;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.TreeMap;
import javax.net.ssl.SSLContext;

import com.aliyun.sdk.service.oss2.transport.AbortableInputStream;
import com.aliyun.sdk.service.oss2.transport.BinaryData;
import com.aliyun.sdk.service.oss2.transport.HttpClient;
import com.aliyun.sdk.service.oss2.transport.RequestContext;
import com.aliyun.sdk.service.oss2.transport.RequestException;
import com.aliyun.sdk.service.oss2.transport.RequestMessage;
import com.aliyun.sdk.service.oss2.transport.ResponseMessage;
import org.apache.hc.client5.http.classic.methods.HttpUriRequestBase;
import org.apache.hc.client5.http.config.ConnectionConfig;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder;
import org.apache.hc.client5.http.protocol.HttpClientContext;
import org.apache.hc.client5.http.ssl.DefaultClientTlsStrategy;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.io.entity.InputStreamEntity;
import org.apache.hc.core5.io.CloseMode;
import org.apache.hc.core5.util.Timeout;
import org.pangrova.common.concurrency.ThreadSafe;
import org.pangrova.common.concurrency.NotThreadSafe;

/** Synchronous SDK transport: verified TLS, no redirects/retries and cancellation before remote-body close. */
@ThreadSafe
final class OssHttpTransport implements HttpClient, AutoCloseable {
    private static final int MAX_CONTROL_RESPONSE_BYTES = 64 * 1024;
    private static final int POOL_WAIT_MILLIS = 5000;
    private final CloseableHttpClient client;
    private final RequestConfig requestConfig;

    OssHttpTransport(OssBlobStorage.Settings settings, SSLContext tlsContext) {
        var connections = PoolingHttpClientConnectionManagerBuilder.create()
                .setTlsSocketStrategy(tlsContext == null ? DefaultClientTlsStrategy.createSystemDefault()
                        : new DefaultClientTlsStrategy(tlsContext))
                .setMaxConnTotal(64).setMaxConnPerRoute(64)
                .setDefaultConnectionConfig(ConnectionConfig.custom()
                        .setConnectTimeout(Timeout.ofMilliseconds(settings.connectTimeout().toMillis()))
                        .setSocketTimeout(Timeout.ofMilliseconds(settings.readTimeout().toMillis())).build()).build();
        try {
            client = HttpClients.custom().setConnectionManager(connections).disableAutomaticRetries()
                    .disableRedirectHandling().disableContentCompression().disableCookieManagement().build();
        } catch (RuntimeException | Error failure) {
            connections.close(CloseMode.IMMEDIATE);
            throw failure;
        }
        requestConfig = RequestConfig.custom().setConnectionRequestTimeout(Timeout.ofMilliseconds(POOL_WAIT_MILLIS))
                .setResponseTimeout(Timeout.ofMilliseconds(settings.readTimeout().toMillis()))
                .setRedirectsEnabled(false).setAuthenticationEnabled(false).setContentCompressionEnabled(false).build();
    }

    @Override
    public ResponseMessage send(RequestMessage request, RequestContext context) {
        HttpUriRequestBase outgoing = new HttpUriRequestBase(request.method(), request.uri());
        outgoing.setConfig(requestConfig);
        request.headers().forEach((name, value) -> {
            if (!"Content-Length".equalsIgnoreCase(name)) outgoing.addHeader(name, value);
        });
        if (request.body() != null) {
            Long length = request.body().getLength();
            outgoing.setEntity(new InputStreamEntity(request.body().toStream(), length == null ? -1 : length, null));
        }
        try {
            ClassicHttpResponse response = client.executeOpen(null, outgoing, HttpClientContext.create());
            try {
                Map<String, String> headers = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
                for (var header : response.getHeaders()) headers.put(header.getName(), header.getValue());
                InputStream content = response.getEntity() == null
                        ? InputStream.nullInputStream() : response.getEntity().getContent();
                var body = new ResponseBody(content, outgoing, response);
                BinaryData data;
                if (response.getCode() / 100 == 2 && RequestContext.HttpCompletionOption.ResponseHeadersRead.equals(
                        context.get(RequestContext.Key.HTTP_COMPLETION_OPTION))) {
                    data = BinaryData.fromStream(AbortableInputStream.create(body, body::abort));
                } else {
                    try (body) {
                        byte[] bytes = body.readNBytes(MAX_CONTROL_RESPONSE_BYTES + 1);
                        if (bytes.length > MAX_CONTROL_RESPONSE_BYTES) {
                            body.abort();
                            throw new IOException("OSS control response exceeds its byte limit");
                        }
                        data = BinaryData.fromBytes(bytes);
                    }
                }
                return ResponseMessage.newBuilder().request(request).statusCode(response.getCode())
                        .headers(headers).body(data).build();
            } catch (IOException | RuntimeException | Error failure) {
                try {
                    abort(outgoing, response);
                } catch (RuntimeException | Error cleanup) {
                    if (cleanup != failure) failure.addSuppressed(cleanup);
                }
                throw failure;
            }
        } catch (IOException failure) {
            throw new RequestException("OSS HTTP request failed", failure);
        }
    }

    private static void abort(HttpUriRequestBase request, ClassicHttpResponse response) {
        request.cancel(); // Closing the live socket first prevents graceful entity close from draining it.
        response.setEntity(null); // The cancelled socket owns no reusable entity; do not read it during close.
        try {
            response.close();
        } catch (IOException failure) {
            throw new UncheckedIOException(failure);
        }
    }

    @Override public String name() { return "Pangrova OSS synchronous HTTP"; }
    @Override public void close() { client.close(CloseMode.IMMEDIATE); }

    @NotThreadSafe
    private static final class ResponseBody extends FilterInputStream {
        private final HttpUriRequestBase request;
        private final ClassicHttpResponse response;
        private boolean ended;

        private ResponseBody(InputStream content, HttpUriRequestBase request, ClassicHttpResponse response) {
            super(content);
            this.request = request;
            this.response = response;
        }

        private void abort() {
            if (!ended) {
                ended = true;
                OssHttpTransport.abort(request, response);
            }
        }

        @Override
        public void close() throws IOException {
            if (!ended) {
                ended = true;
                response.close();
            }
        }
    }
}

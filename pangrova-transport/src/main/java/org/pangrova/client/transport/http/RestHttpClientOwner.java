package org.pangrova.client.transport.http;

import org.pangrova.common.concurrent.ManagedExecutorFactory;
import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.common.concurrent.GuardedBy;

import java.net.http.HttpClient;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/** Owns configured HTTPS context construction and any explicitly allocated HTTP executor. */
@ThreadSafe
final class RestHttpClientOwner implements AutoCloseable {
    private final HttpClient client;
    private final ExecutorService executor;
    @GuardedBy("this")
    private boolean sealed;
    @GuardedBy("this")
    private int requests;

    private RestHttpClientOwner(HttpClient client, ExecutorService executor) {
        this.client = client;
        this.executor = executor;
    }

    static RestHttpClientOwner create(PangrovaRestClientConfig config, HttpClient shared) {
        if (shared != null && config.trustStorePath() != null) {
            throw new IllegalArgumentException("A shared HTTP client cannot override a configured HTTPS trust store");
        }
        if (shared != null && config.httpExecutorThreads() != null) {
            throw new IllegalArgumentException("A shared HTTP client cannot override a configured HTTP executor size");
        }
        if (shared != null) return new RestHttpClientOwner(shared, null);
        var builder = HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NEVER)
                .connectTimeout(config.connectTimeout());
        if (config.trustStorePath() != null) builder.sslContext(tlsContext(config));
        var pool = config.connectionPoolConfig();
        Integer threads = config.httpExecutorThreads() != null ? config.httpExecutorThreads()
                : pool == null ? null : Math.multiplyExact(pool.maxConnectionsPerEndpoint(), pool.endpoints().size());
        ExecutorService executor = threads == null ? null
                : ManagedExecutorFactory.newFixedThreadPool("pangrova-rest-pool", threads);
        try {
            if (executor != null) builder.executor(executor);
            return new RestHttpClientOwner(builder.build(), executor);
        } catch (RuntimeException | Error failure) {
            try {
                closeExecutor(executor);
            } catch (RuntimeException cleanupFailure) {
                failure.addSuppressed(cleanupFailure);
            }
            throw failure;
        }
    }

    HttpClient client() { return client; }

    synchronized void seal() { sealed = true; }

    synchronized RequestOperation enter() {
        if (sealed) throw new IllegalStateException("HTTP client admission is closed");
        var operation = new RequestOperation();
        requests = Math.incrementExact(requests);
        return operation;
    }

    private static javax.net.ssl.SSLContext tlsContext(PangrovaRestClientConfig config) {
        char[] password = config.trustStorePassword() == null ? null : config.trustStorePassword().toCharArray();
        try {
            var store = java.security.KeyStore.getInstance(config.trustStorePath().endsWith(".p12") ? "PKCS12" : "JKS");
            try (var input = java.nio.file.Files.newInputStream(java.nio.file.Path.of(config.trustStorePath()))) {
                store.load(input, password);
            }
            var trust = javax.net.ssl.TrustManagerFactory.getInstance(
                    javax.net.ssl.TrustManagerFactory.getDefaultAlgorithm());
            trust.init(store);
            var context = javax.net.ssl.SSLContext.getInstance("TLS");
            context.init(null, trust.getTrustManagers(), null);
            return context;
        } catch (Exception failure) {
            throw new IllegalStateException("Configured HTTPS trust store could not be loaded", failure);
        } finally {
            if (password != null) java.util.Arrays.fill(password, '\0');
        }
    }

    @Override public void close() {
        synchronized (this) {
            sealed = true;
            if (requests != 0) {
                throw new IllegalStateException("HTTP requests are still owned; retry close after they exit");
            }
        }
        closeExecutor(executor);
    }

    final class RequestOperation implements AutoCloseable {
        @GuardedBy("RestHttpClientOwner.this")
        private boolean returned;

        @Override public void close() {
            synchronized (RestHttpClientOwner.this) {
                if (returned) return;
                returned = true;
                requests--;
            }
        }
    }

    private static void closeExecutor(ExecutorService executor) {
        if (executor == null) return;
        executor.shutdown();
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                throw new IllegalStateException("HTTP executor is still owned; retry close after actual requests exit");
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("HTTP executor close was interrupted; ownership is retained", interrupted);
        }
    }
}

package org.pangrova.testing.testkit.security;

import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsServer;
import org.pangrova.common.concurrency.ManagedExecutorFactory;
import org.pangrova.common.concurrency.NotThreadSafe;

import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyFactory;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Base64;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;

/** Real JDK HTTPS health endpoint and JKS fixtures; transport component proof, not SQL execution. */
@NotThreadSafe
public final class TestHttpsEndpoint implements AutoCloseable {
    private final ExecutorService executor = ManagedExecutorFactory.newFixedThreadPool("test-https-health", 2);
    private final AtomicInteger requests = new AtomicInteger();
    private final java.util.concurrent.atomic.AtomicReference<RequestBlock> nextBlock =
            new java.util.concurrent.atomic.AtomicReference<>();
    private final java.util.Set<RequestBlock> blocks = java.util.concurrent.ConcurrentHashMap.newKeySet();
    private HttpsServer server;
    private boolean closed;

    /** Starts an ephemeral server with an actual CA-signed test identity. */
    public TestHttpsEndpoint(TestTlsAuthority authority, TestTlsAuthority.Identity identity) throws Exception {
        try {
            server = HttpsServer.create(new InetSocketAddress("localhost", 0), 0);
            server.setHttpsConfigurator(new HttpsConfigurator(serverContext(authority.root(), identity)));
            server.setExecutor(executor);
            server.createContext("/v1/health", exchange -> {
                try (exchange) {
                    requests.incrementAndGet();
                    RequestBlock block = nextBlock.getAndSet(null);
                    if (block != null) block.awaitRelease();
                    byte[] body = "healthy".getBytes(java.nio.charset.StandardCharsets.US_ASCII);
                    exchange.sendResponseHeaders(200, body.length);
                    exchange.getResponseBody().write(body);
                }
            });
            server.start();
        } catch (Exception | Error failure) {
            try {
                close();
            } catch (Exception cleanupFailure) {
                failure.addSuppressed(cleanupFailure);
            }
            throw failure;
        }
    }

    /** Returns the actual OS-assigned port. */
    public int port() { return server.getAddress().getPort(); }

    /** Counts HTTP requests that passed the actual TLS handshake. */
    public int requests() { return requests.get(); }

    /** Holds one actual HTTP response after a successful TLS handshake, without changing its bytes. */
    public RequestBlock blockNextRequest() {
        var block = new RequestBlock();
        if (!nextBlock.compareAndSet(null, block)) {
            throw new IllegalStateException("Another actual request is already selected");
        }
        blocks.add(block);
        return block;
    }

    /** Writes a real JKS trust store containing exactly the supplied CA certificate. */
    public static void writeTrustStore(Path destination, Path root, char[] password) throws Exception {
        var store = KeyStore.getInstance("JKS");
        store.load(null, password);
        store.setCertificateEntry("test-root", certificate(root));
        try (var output = Files.newOutputStream(destination)) {
            store.store(output, password);
        }
    }

    private static SSLContext serverContext(Path root, TestTlsAuthority.Identity identity) throws Exception {
        char[] password = UUID.randomUUID().toString().toCharArray();
        byte[] encoded = privateKey(identity.privateKey());
        try {
            var key = KeyFactory.getInstance("RSA").generatePrivate(new PKCS8EncodedKeySpec(encoded));
            var store = KeyStore.getInstance("PKCS12");
            store.load(null, password);
            store.setKeyEntry("server", key, password,
                    new Certificate[] {certificate(identity.certificateChain()), certificate(root)});
            var keys = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            keys.init(store, password);
            var context = SSLContext.getInstance("TLS");
            context.init(keys.getKeyManagers(), null, null);
            return context;
        } finally {
            java.util.Arrays.fill(password, '\0');
            java.util.Arrays.fill(encoded, (byte) 0);
        }
    }

    private static byte[] privateKey(Path path) throws Exception {
        String pem = Files.readString(path, java.nio.charset.StandardCharsets.US_ASCII);
        String body = pem.replace("-----BEGIN PRIVATE KEY-----", "").replace("-----END PRIVATE KEY-----", "");
        return Base64.getMimeDecoder().decode(body);
    }

    private static Certificate certificate(Path path) throws Exception {
        try (var input = Files.newInputStream(path)) {
            return CertificateFactory.getInstance("X.509").generateCertificate(input);
        }
    }

    /** Stops the real server and joins its request executor; failed joins remain retryable. */
    @Override
    public void close() throws Exception {
        if (closed) return;
        blocks.forEach(RequestBlock::close);
        Exception failure = null;
        try {
            if (server != null) server.stop(0);
        } catch (RuntimeException cleanupFailure) {
            failure = cleanupFailure;
        }
        executor.shutdown();
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Actual HTTPS request threads have not exited");
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            if (failure != null) interrupted.addSuppressed(failure);
            failure = interrupted;
        } catch (RuntimeException cleanupFailure) {
            if (failure == null) failure = cleanupFailure;
            else failure.addSuppressed(cleanupFailure);
        }
        if (failure != null) throw failure;
        closed = true;
    }

    /** Owns a real response barrier; closing only releases the server's original response. */
    @org.pangrova.common.concurrency.ThreadSafe
    public final class RequestBlock implements AutoCloseable {
        private final java.util.concurrent.CountDownLatch entered = new java.util.concurrent.CountDownLatch(1);
        private final java.util.concurrent.CountDownLatch release = new java.util.concurrent.CountDownLatch(1);

        /** Waits until an actual HTTPS request reaches the response barrier. */
        public boolean awaitEntered() throws InterruptedException {
            try {
                return entered.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw interrupted;
            }
        }

        private void awaitRelease() throws java.io.IOException {
            entered.countDown();
            try {
                if (!release.await(5, TimeUnit.SECONDS)) {
                    throw new java.io.IOException("Actual HTTPS response remains held");
                }
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new java.io.IOException("Actual HTTPS response was interrupted", interrupted);
            }
        }

        @Override
        public void close() {
            release.countDown();
            blocks.remove(this);
        }
    }
}

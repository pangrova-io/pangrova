package org.pangrova.storage.oss.blob;

import com.aliyun.sdk.service.oss2.hash.CRC64;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsServer;
import org.pangrova.common.concurrent.ManagedExecutorFactory;
import org.pangrova.testing.testkit.security.TestTlsAuthority;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyFactory;
import java.security.KeyStore;
import java.security.MessageDigest;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.spec.PKCS8EncodedKeySpec;
import java.time.Duration;
import java.util.Arrays;
import java.util.Base64;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/** Actual SDK/TLS/HTTP protocol fixture. Its object map is not an Alibaba Cloud OSS qualification. */
final class OssHttpFixture implements AutoCloseable {
    final Map<String, Stored> objects = new ConcurrentHashMap<>();
    final AtomicInteger puts = new AtomicInteger();
    final AtomicInteger gets = new AtomicInteger();
    final AtomicInteger versions = new AtomicInteger();
    final URI endpoint;
    volatile String versioning;
    volatile int errorStatus;
    volatile String errorCode;
    volatile String putVersion;
    volatile boolean badUploadCrc;
    volatile Runnable onVersioning;
    volatile PutEvidence lastPut;
    volatile BodyBlock bodyBlock;
    volatile BodyBlock controlBlock;
    private TestTlsAuthority authority;
    private HttpsServer server;
    private final ExecutorService executor = ManagedExecutorFactory.newFixedThreadPool("oss-http-fixture", 2);

    OssHttpFixture(Path directory) throws Exception {
        try {
            authority = new TestTlsAuthority(directory);
            var identity = authority.issue("oss", "localhost", false);
            SSLContext serverTls = serverContext(identity);
            server = HttpsServer.create(new InetSocketAddress("localhost", 0), 0);
            server.setHttpsConfigurator(new HttpsConfigurator(serverTls));
            server.setExecutor(executor);
            server.createContext("/", this::serve);
            server.start();
            endpoint = URI.create("https://localhost:" + server.getAddress().getPort());
        } catch (Exception | Error failure) {
            try { close(); } catch (Exception | Error cleanup) {
                if (cleanup != failure) failure.addSuppressed(cleanup);
            }
            throw failure;
        }
    }

    void reset() {
        if (bodyBlock != null) bodyBlock.close();
        if (controlBlock != null) controlBlock.close();
        objects.clear();
        puts.set(0);
        gets.set(0);
        versions.set(0);
        versioning = null;
        errorStatus = 0;
        errorCode = null;
        putVersion = null;
        badUploadCrc = false;
        onVersioning = null;
        lastPut = null;
        bodyBlock = null;
        controlBlock = null;
    }

    OssBlobStorage.Settings settings(OssBlobStorage.Encryption encryption) {
        return new OssBlobStorage.Settings("test-bucket", "cn-hangzhou", endpoint, "files", encryption,
                true, Duration.ofSeconds(1), Duration.ofSeconds(5));
    }

    private void serve(HttpExchange exchange) throws IOException {
        try (exchange) {
            if (exchange.getRequestURI().getRawQuery() != null
                    && exchange.getRequestURI().getRawQuery().contains("versioning")) {
                versions.incrementAndGet();
                if (onVersioning != null) onVersioning.run();
                BodyBlock block = controlBlock;
                if (block != null) {
                    exchange.sendResponseHeaders(200, 64 * 1024 + 2);
                    exchange.getResponseBody().write(new byte[1024]);
                    exchange.getResponseBody().flush();
                    block.awaitRelease();
                    return;
                }
                String status = versioning == null ? "" : "<Status>" + versioning + "</Status>";
                send(exchange, 200, "<VersioningConfiguration>" + status + "</VersioningConfiguration>");
                return;
            }
            String key = exchange.getRequestURI().getRawPath();
            if ("PUT".equals(exchange.getRequestMethod())) {
                put(exchange, key);
                return;
            }
            Stored object = objects.get(key);
            if (object == null) {
                error(exchange, 404, "NoSuchKey");
                return;
            }
            object.metadata().forEach((name, value) -> exchange.getResponseHeaders().set("x-oss-meta-" + name, value));
            exchange.getResponseHeaders().set("Content-Length", Long.toString(object.length()));
            if (object.encryption() != null) {
                exchange.getResponseHeaders().set("x-oss-server-side-encryption", object.encryption());
            }
            if (object.kmsKey() != null) {
                exchange.getResponseHeaders().set("x-oss-server-side-encryption-key-id", object.kmsKey());
            }
            if (object.version() != null) exchange.getResponseHeaders().set("x-oss-version-id", object.version());
            if ("HEAD".equals(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(200, -1);
            } else {
                gets.incrementAndGet();
                exchange.sendResponseHeaders(200, object.length() == 0 ? -1 : object.length());
                BodyBlock block = bodyBlock;
                if (block != null) block.awaitRelease();
                exchange.getResponseBody().write(object.bytes());
            }
        }
    }

    private void put(HttpExchange exchange, String key) throws IOException {
        puts.incrementAndGet();
        byte[] content = exchange.getRequestBody().readAllBytes();
        var headers = exchange.getRequestHeaders();
        lastPut = new PutEvidence(key, headers.getFirst("Authorization"), headers.getFirst("x-oss-forbid-overwrite"),
                headers.getFirst("Content-Md5"), headers.getFirst("Content-Length"));
        if (errorStatus != 0) {
            error(exchange, errorStatus, errorCode);
            return;
        }
        if (objects.containsKey(key)) {
            error(exchange, 409, "FileAlreadyExists");
            return;
        }
        String expectedMd5;
        try {
            expectedMd5 = Base64.getEncoder().encodeToString(MessageDigest.getInstance("MD5").digest(content));
        } catch (java.security.NoSuchAlgorithmException failure) {
            throw new IOException(failure);
        }
        if (!expectedMd5.equals(lastPut.md5()) || !"true".equals(lastPut.forbidOverwrite())) {
            error(exchange, 400, "InvalidArgument");
            return;
        }
        objects.put(key, new Stored(content, content.length,
                Map.of("sha256", headers.getFirst("x-oss-meta-sha256"), "size", headers.getFirst("x-oss-meta-size")),
                headers.getFirst("x-oss-server-side-encryption"), headers.getFirst("x-oss-server-side-encryption-key-id"),
                putVersion));
        if (putVersion != null) exchange.getResponseHeaders().set("x-oss-version-id", putVersion);
        var crc = new CRC64();
        crc.update(content, 0, content.length);
        exchange.getResponseHeaders().set("x-oss-hash-crc64ecma",
                badUploadCrc ? "0" : Long.toUnsignedString(crc.getValue()));
        exchange.sendResponseHeaders(200, -1);
    }

    private static void error(HttpExchange exchange, int status, String code) throws IOException {
        send(exchange, status, "<Error><Code>" + code + "</Code><Message>protocol fixture</Message></Error>");
    }

    private static void send(HttpExchange exchange, int status, String text) throws IOException {
        byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/xml");
        exchange.sendResponseHeaders(status, bytes.length);
        exchange.getResponseBody().write(bytes);
    }

    private SSLContext serverContext(TestTlsAuthority.Identity identity) throws Exception {
        char[] password = UUID.randomUUID().toString().toCharArray();
        String pem = Files.readString(identity.privateKey()).replace("-----BEGIN PRIVATE KEY-----", "")
                .replace("-----END PRIVATE KEY-----", "");
        byte[] keyBytes = Base64.getMimeDecoder().decode(pem);
        try {
            var key = KeyFactory.getInstance("RSA").generatePrivate(new PKCS8EncodedKeySpec(keyBytes));
            var store = KeyStore.getInstance("PKCS12");
            store.load(null, password);
            store.setKeyEntry("server", key, password,
                    new Certificate[]{certificate(identity.certificateChain()), certificate(authority.root())});
            var keys = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            keys.init(store, password);
            SSLContext context = SSLContext.getInstance("TLS");
            context.init(keys.getKeyManagers(), null, null);
            return context;
        } finally {
            Arrays.fill(keyBytes, (byte) 0);
            Arrays.fill(password, '\0');
        }
    }

    private X509TrustManager trustManager() throws Exception {
        var store = KeyStore.getInstance("JKS");
        store.load(null, null);
        store.setCertificateEntry("test-ca", certificate(authority.root()));
        var trust = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trust.init(store);
        return (X509TrustManager) trust.getTrustManagers()[0];
    }

    SSLContext trustContext() throws Exception {
        SSLContext context = SSLContext.getInstance("TLS");
        context.init(null, new javax.net.ssl.TrustManager[]{trustManager()}, null);
        return context;
    }

    private static Certificate certificate(Path file) throws Exception {
        try (var input = Files.newInputStream(file)) {
            return CertificateFactory.getInstance("X.509").generateCertificate(input);
        }
    }

    @Override
    public void close() throws Exception {
        if (bodyBlock != null) bodyBlock.close();
        if (controlBlock != null) controlBlock.close();
        Throwable failure = null;
        for (AutoCloseable owner : new AutoCloseable[]{() -> { if (server != null) server.stop(0); },
                () -> {
                    executor.shutdownNow();
                    try {
                        if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                            throw new IllegalStateException("OSS fixture requests did not exit");
                        }
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        throw interrupted;
                    }
                }, authority}) {
            if (owner == null) continue;
            try { owner.close(); } catch (Exception | Error cleanup) {
                if (failure == null) failure = cleanup;
                else if (failure != cleanup) failure.addSuppressed(cleanup);
            }
        }
        if (failure instanceof Exception checked) throw checked;
        if (failure instanceof Error fatal) throw fatal;
    }

    record Stored(byte[] bytes, long length, Map<String, String> metadata,
                  String encryption, String kmsKey, String version) {}
    record PutEvidence(String key, String authorization, String forbidOverwrite, String md5, String length) {}

    static final class BodyBlock implements AutoCloseable {
        final CountDownLatch entered = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);

        private void awaitRelease() throws IOException {
            entered.countDown();
            try {
                if (!release.await(5, TimeUnit.SECONDS)) throw new IOException("OSS fixture body remained blocked");
            } catch (InterruptedException failure) {
                Thread.currentThread().interrupt();
                throw new IOException(failure);
            }
        }

        @Override public void close() { release.countDown(); }
    }
}

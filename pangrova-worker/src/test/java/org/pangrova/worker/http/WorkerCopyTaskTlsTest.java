package org.pangrova.worker.http;

import com.google.protobuf.ByteString;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.common.http.PemMutualTls;
import org.pangrova.common.json.JsonMapperFactory;
import org.pangrova.protocol.rest.copy.CopySourceReadHttp;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.v2.Checksum;
import org.pangrova.protocol.v2.TaskKind;
import org.pangrova.testing.TestExchangeTransport;
import org.pangrova.testing.nativefixture.CanonicalTaskRequestTestFixture;
import org.pangrova.worker.task.TaskManager;
import org.pangrova.worker.bootstrap.WorkerBootstrapConfig;
import org.pangrova.worker.bootstrap.WorkerComponentRefs;
import org.pangrova.worker.bootstrap.phase.WorkerHeartbeatBootstrap;

import java.net.URI;
import java.net.InetSocketAddress;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.util.HexFormat;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real Jetty and JSSE admission checks for the private COPY control listener. */
class WorkerCopyTaskTlsTest {
    @TempDir Path directory;
    private WorkerHttpServer server;
    private TaskManager taskManager;
    private ch.qos.logback.classic.Level priorJettyLevel;

    @BeforeEach
    void protectPrivateTicketWireLogging() {
        var jetty = (ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory
                .getLogger("org.eclipse.jetty");
        priorJettyLevel = jetty.getLevel();
        jetty.setLevel(ch.qos.logback.classic.Level.WARN);
    }

    @AfterEach
    void stop() throws Exception {
        try {
            if (server != null) server.stop();
            if (taskManager != null) taskManager.close();
        } finally {
            ((ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory
                    .getLogger("org.eclipse.jetty")).setLevel(priorJettyLevel);
        }
    }

    @Test
    void debugWireLoggingCannotStartPrivateTicketTransport() {
        var jetty = (ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory
                .getLogger("org.eclipse.jetty");
        jetty.setLevel(ch.qos.logback.classic.Level.DEBUG);
        assertThatThrownBy(CopyTaskWireLoggingPolicy::requireSafeJettyLogging)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Jetty logging");
    }

    @Test
    void privateCopyRequiresMutualTlsAndPlainListenerRejectsTicket() throws Exception {
        CertificateFiles files = createCertificates();
        taskManager = TaskManager.forHttpWiringOnly();
        taskManager.requireControlEpochHandshake();
        server = new WorkerHttpServer(
                new WorkerHttpServer.WorkerHttpConfig(0, "localhost", 0, "test-secret", "grpc",
                        new WorkerHttpServer.CopyTaskTls(files.serverCert(), files.serverKey(), files.ca())),
                taskManager, TestExchangeTransport.INSTANCE, null, null, null,
                new org.pangrova.worker.copy.CopyStageLeases((nonce, scope, budget) -> {
                    throw new AssertionError("fenced HTTP probe must not stage a source");
                }, (nonce, scope, proof) -> proof, java.time.Clock.systemUTC(), null));
        server.start();
        assertThat(server.getCopyTaskTlsPort()).isPositive();

        byte[] body = JsonMapperFactory.shared().writeValueAsBytes(copyTask());
        HttpClient trusted = HttpClient.newBuilder().sslContext(PemMutualTls.context(
                files.clientCert(), files.clientKey(), files.ca())).build();
        HttpRequest privateRequest = request("https://localhost:" + server.getCopyTaskTlsPort(), body);
        HttpResponse<String> acceptedTransport = trusted.send(privateRequest,
                HttpResponse.BodyHandlers.ofString());
        // Transport admission succeeds, but the existing epoch/staging gates
        // still reject this deliberately uncommitted test task.
        assertThat(acceptedTransport.statusCode()).isEqualTo(503);
        HttpResponse<String> stageTransport = trusted.send(
                HttpRequest.newBuilder(URI.create("https://localhost:"
                                + server.getCopyTaskTlsPort() + "/v1/task/copy-stage"))
                        .header("Content-Type", "application/json")
                        .header("X-Internal-Auth", "test-secret")
                        .POST(HttpRequest.BodyPublishers.ofByteArray(preparationBody())).build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(stageTransport.statusCode()).isEqualTo(503);
        HttpResponse<String> wrongServiceIdentity = trusted.send(
                HttpRequest.newBuilder(URI.create("https://localhost:"
                                + server.getCopyTaskTlsPort() + "/v1/task"))
                        .header("Content-Type", "application/json")
                        .header("X-Internal-Auth", "wrong-secret")
                        .POST(HttpRequest.BodyPublishers.ofByteArray(body)).build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(wrongServiceIdentity.statusCode()).isEqualTo(401);

        HttpResponse<String> plaintext = HttpClient.newHttpClient().send(
                request("http://localhost:" + server.getPort(), body),
                HttpResponse.BodyHandlers.ofString());
        assertThat(plaintext.statusCode()).isEqualTo(403);
        assertThat(plaintext.body()).contains("COPY_SECURE_TRANSPORT_REQUIRED");

        HttpResponse<String> rejectedBeforeJson = HttpClient.newHttpClient().send(
                HttpRequest.newBuilder(URI.create("http://localhost:" + server.getPort()
                                + "/v1/task/copy-stage"))
                        .header("X-Internal-Auth", "test-secret")
                        .header("X-Forwarded-Proto", "https")
                        .POST(HttpRequest.BodyPublishers.ofString("{malformed bearer"))
                        .build(), HttpResponse.BodyHandlers.ofString());
        assertThat(rejectedBeforeJson.statusCode()).isEqualTo(403);

        // Trust the CA while withholding a client certificate: only the mTLS
        // client-auth requirement can account for this handshake failure.
        HttpClient noClientIdentity = HttpClient.newBuilder().sslContext(
                trustOnly(files.ca())).build();
        assertThatThrownBy(() -> noClientIdentity.send(privateRequest,
                HttpResponse.BodyHandlers.ofString())).isInstanceOf(Exception.class);
        assertThatThrownBy(() -> trusted.send(
                request("https://127.0.0.1:" + server.getCopyTaskTlsPort(), body),
                HttpResponse.BodyHandlers.ofString())).isInstanceOf(Exception.class);
        assertThatThrownBy(() -> HttpClient.newHttpClient().send(
                request("http://localhost:" + server.getCopyTaskTlsPort(), body),
                HttpResponse.BodyHandlers.ofString())).isInstanceOf(Exception.class);
    }

    @Test
    void fixedSecurePortIsAdvertisedAsActualRegistrationPort() throws Exception {
        CertificateFiles files = createCertificates();
        int configuredPort;
        try (var probe = new java.net.ServerSocket(0)) {
            configuredPort = probe.getLocalPort();
        }
        taskManager = TaskManager.forHttpWiringOnly();
        server = new WorkerHttpServer(
                new WorkerHttpServer.WorkerHttpConfig(0, "localhost", 0, "test-secret", "grpc",
                        new WorkerHttpServer.CopyTaskTls(files.serverCert(), files.serverKey(),
                                files.ca(), configuredPort)),
                taskManager, TestExchangeTransport.INSTANCE);
        server.start();
        assertThat(server.getCopyTaskTlsPort()).isEqualTo(configuredPort);

        AtomicReference<String> registration = new AtomicReference<>();
        var coordinator = com.sun.net.httpserver.HttpServer.create(
                new InetSocketAddress("127.0.0.1", 0), 0);
        coordinator.createContext("/v1/workers/register", exchange -> {
            registration.set(new String(exchange.getRequestBody().readAllBytes()));
            byte[] response = "{\"status\":\"registered\",\"coordinator_epoch\":1}".getBytes();
            exchange.sendResponseHeaders(200, response.length);
            try (var output = exchange.getResponseBody()) {
                output.write(response);
            }
        });
        coordinator.start();
        WorkerComponentRefs refs = new WorkerComponentRefs();
        refs.setHttpServer(server);
        refs.setNativeCapabilityInventory(org.pangrova.protocol.CapabilityInventoryCodec.encode(
                org.pangrova.protocol.v2.CapabilityInventory.parseFrom(
                        org.pangrova.testing.testkit.function.ActualNativeCatalogue.bytes())));
        var defaults = WorkerBootstrapConfig.withDefaults("w-copy");
        var config = new WorkerBootstrapConfig(
                "w-copy", server.getPort(), defaults.transportPort(),
                "http://127.0.0.1:" + coordinator.getAddress().getPort(),
                defaults.heartbeatIntervalMs(), defaults.observabilityEnabled(),
                defaults.observabilityBootstrapConfig(), defaults.resourceConfig(),
                defaults.resultRetentionSeconds(), defaults.cdcEnabled(),
                defaults.registrationRetryConfig(), "localhost", "test-secret",
                defaults.shuffleMode(), defaults.cacheConfig(), "localhost",
                defaults.shuffleAdvertisedHost(), defaults.nativeFunctionConfig(),
                defaults.transportSecurityConfig());
        try {
            WorkerHeartbeatBootstrap.register(config, refs);
            assertThat(registration.get()).isNotNull();
            var request = JsonMapperFactory.shared().readValue(registration.get(),
                    org.pangrova.protocol.rest.worker.WorkerRegistrationRequest.class);
            assertThat(request.copyTaskTlsPort()).isEqualTo(configuredPort);
        } finally {
            if (refs.registrationClient() != null) refs.registrationClient().close();
            coordinator.stop(0);
        }
    }

    private static HttpRequest request(String base, byte[] body) {
        return HttpRequest.newBuilder(URI.create(base + "/v1/task"))
                .header("Content-Type", "application/json")
                .header("X-Internal-Auth", "test-secret")
                .POST(HttpRequest.BodyPublishers.ofByteArray(body)).build();
    }

    private static javax.net.ssl.SSLContext trustOnly(Path ca) throws Exception {
        var certificates = java.security.cert.CertificateFactory.getInstance("X.509");
        var store = java.security.KeyStore.getInstance("PKCS12");
        store.load(null, null);
        try (var input = java.nio.file.Files.newInputStream(ca)) {
            store.setCertificateEntry("ca", certificates.generateCertificate(input));
        }
        var trust = javax.net.ssl.TrustManagerFactory.getInstance(
                javax.net.ssl.TrustManagerFactory.getDefaultAlgorithm());
        trust.init(store);
        var context = javax.net.ssl.SSLContext.getInstance("TLS");
        context.init(null, trust.getTrustManagers(), null);
        return context;
    }

    private static SubmitTaskRequest copyTask() {
        var scope = new CopySourceReadHttp.Scope("tenant", "test-user", "incoming",
                "f92e704f-2443-4d3a-a496-cd6507dd97db", 1,
                "s3://incoming/file.parquet", "s3://managed/copy", "a".repeat(64),
                "query-7", "task-42", "coordinator", "test", 1, 1,
                4_102_444_800_000L);
        var task = CanonicalTaskRequestTestFixture.builder("task-42", "query-7", 0, 0,
                        TaskKind.TASK_KIND_WRITE, new byte[] {1})
                .setCopySourceStagingCommandSha256(Checksum.newBuilder()
                        .setAlgorithm(Checksum.Algorithm.ALGORITHM_SHA256)
                        .setDigest(ByteString.copyFrom(HexFormat.of().parseHex(scope.commandSha256()))))
                .setResourceGrant(org.pangrova.protocol.v2.StageResourceGrant.newBuilder()
                        .setGrantId("task-42:grant").setCpuSlots(1)
                        .setNativeMemoryBytes(64L * 1024 * 1024))
                .build();
        return SubmitTaskRequest.from(task, new CopySourceReadHttp.RedeemRequest(
                "n".repeat(43), scope));
    }

    private static byte[] preparationBody() throws Exception {
        var nativeRequest = copyTask();
        var task = nativeRequest.taskRequest();
        var preparation = org.pangrova.protocol.v2.SourcePreparationRequest.newBuilder()
                .setProtocol(task.getProtocol()).setAttempt(task.getAttempt()).setSecurity(task.getSecurity())
                .setDeadlineEpochMs(task.getDeadlineEpochMs())
                .setSourceCommandSha256(task.getCopySourceStagingCommandSha256())
                .setResourceGrant(task.getResourceGrant().toBuilder().clearNativeMemoryBytes().setJvmHeapBytes(1024 * 1024))
                .build();
        return JsonMapperFactory.shared().writeValueAsBytes(org.pangrova.protocol.rest.copy.PrepareCopySourceRequest
                .from(preparation, nativeRequest.copySourceRead()));
    }

    private CertificateFiles createCertificates() throws Exception {
        Path ca = directory.resolve("ca.pem");
        Path caKey = directory.resolve("ca.key");
        run("openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes",
                "-keyout", caKey.toString(), "-out", ca.toString(),
                "-subj", "/CN=Pangrova test CA", "-days", "1");
        Path serverCert = issue("server", ca, caKey, "subjectAltName=DNS:localhost");
        Path clientCert = issue("client", ca, caKey, "extendedKeyUsage=clientAuth");
        return new CertificateFiles(ca, serverCert, directory.resolve("server.key"),
                clientCert, directory.resolve("client.key"));
    }

    private Path issue(String name, Path ca, Path caKey, String extension) throws Exception {
        Path key = directory.resolve(name + ".key");
        Path csr = directory.resolve(name + ".csr");
        Path cert = directory.resolve(name + ".pem");
        run("openssl", "genpkey", "-algorithm", "RSA", "-out", key.toString(),
                "-pkeyopt", "rsa_keygen_bits:2048");
        run("openssl", "req", "-new", "-key", key.toString(), "-out", csr.toString(),
                "-subj", "/CN=" + name);
        run("openssl", "x509", "-req", "-in", csr.toString(), "-CA", ca.toString(),
                "-CAkey", caKey.toString(), "-CAcreateserial", "-out", cert.toString(),
                "-days", "1", "-extfile", writeExtension(name, extension).toString());
        return cert;
    }

    private Path writeExtension(String name, String extension) throws Exception {
        Path path = directory.resolve(name + ".ext");
        java.nio.file.Files.writeString(path, extension + System.lineSeparator());
        return path;
    }

    private static void run(String... command) throws Exception {
        Process process = new ProcessBuilder(List.of(command)).redirectErrorStream(true).start();
        String output = new String(process.getInputStream().readAllBytes());
        if (process.waitFor() != 0) throw new IllegalStateException(output);
    }

    private record CertificateFiles(Path ca, Path serverCert, Path serverKey,
                                    Path clientCert, Path clientKey) {}
}

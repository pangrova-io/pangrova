package org.pangrova.testing.integration.e2e.speculation;

import com.fasterxml.jackson.databind.JsonNode;
import org.awaitility.Awaitility;
import org.pangrova.client.transport.http.PangrovaRestClient;
import org.pangrova.client.transport.security.ClientAuthentication;
import org.pangrova.common.json.provider.jackson.JsonMapperFactory;
import org.pangrova.protocol.capability.inventory.CapabilityInventoryCodec;
import org.pangrova.client.core.protocol.QueryStatusResponse;
import org.pangrova.client.core.protocol.ResultProtocol;
import org.pangrova.protocol.rest.worker.WorkerRegistrationRequest;
import org.pangrova.protocol.rest.worker.WorkerControlResponse;
import org.pangrova.testing.integration.e2e.SharedTestContainers;
import org.pangrova.testing.testkit.TestProjectPaths;
import org.pangrova.testing.testkit.security.TestTlsAuthority;
import org.pangrova.scheduler.store.jdbc.admission.JdbcClusterConcurrencySlots;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Inet4Address;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/** Three production Main classes in three owned local JVMs; the test JVM never owns an engine. */
final class IndependentLocalProcesses implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(IndependentLocalProcesses.class);
    private static final Duration STARTUP_TIMEOUT = Duration.ofSeconds(120);
    private final Path workspace;
    private final String internalSecret = UUID.randomUUID().toString();
    private final Path warehouse;
    private final Path javaExecutable;
    private final LocalRuntimeArtifacts artifacts;
    private final org.pangrova.worker.nativebridge.jni.NativeFunctionConfig functions;
    private final Inet4Address advertisedAddress;
    private final boolean speculation;
    private final ProcessSecurityAuthority security;
    private final ProcessStatusReportRelay reports;
    private final TestTlsAuthority tls;
    private final TestTlsAuthority.Identity coordinatorIdentity;
    private final TestTlsAuthority.Identity clientIdentity;
    private final Map<String, TestTlsAuthority.Identity> workerIdentities = new LinkedHashMap<>();
    private final Map<String, OwnedProcess> workers = new LinkedHashMap<>();
    private final Map<String, RegisteredWorker> acceptedWorkers = new LinkedHashMap<>();
    private final List<OwnedProcess> owned = new java.util.concurrent.CopyOnWriteArrayList<>();
    private final HttpClient http = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(2)).build();
    private OwnedProcess coordinator;
    private final int coordinatorPort;
    private boolean closed;
    private boolean preserveDiagnostics;
    private boolean reportsClosed;
    private boolean securityClosed;
    private boolean tlsClosed;

    IndependentLocalProcesses() throws Exception {
        this(org.pangrova.worker.nativebridge.jni.NativeFunctionConfig.DEFAULT);
    }

    IndependentLocalProcesses(org.pangrova.worker.nativebridge.jni.NativeFunctionConfig functions) throws Exception {
        this(functions, false);
    }

    IndependentLocalProcesses(boolean speculation) throws Exception {
        this(org.pangrova.worker.nativebridge.jni.NativeFunctionConfig.DEFAULT, speculation);
    }

    private IndependentLocalProcesses(org.pangrova.worker.nativebridge.jni.NativeFunctionConfig functions,
                                      boolean speculation) throws Exception {
        this.speculation = speculation;
        this.functions = java.util.Objects.requireNonNull(functions, "functions");
        if (!functions.autoRegister()) {
            throw new IllegalArgumentException("Independent Workers must register their actual Native functions");
        }
        if (Runtime.version().feature() != 17) {
            throw new IllegalStateException("Independent local process test requires JDK 17");
        }
        if (!"UTF-8".equalsIgnoreCase(System.getProperty("native.encoding"))) {
            throw new IllegalStateException("Independent local process test requires a UTF-8 "
                    + "native locale so non-ASCII CLI credentials survive ProcessBuilder");
        }
        javaExecutable = Path.of(System.getProperty("java.home"), "bin", "java");
        artifacts = LocalRuntimeArtifacts.fromRequiredProperties();
        advertisedAddress = routableHostAddress();
        workspace = Files.createTempDirectory("pangrova-local-process-",
                PosixFilePermissions.asFileAttribute(
                        PosixFilePermissions.fromString("rwx------")));
        Path createdWarehouse;
        ProcessSecurityAuthority createdSecurity = null;
        ProcessStatusReportRelay createdReports = null;
        TestTlsAuthority createdTls = null;
        TestTlsAuthority.Identity createdClient;
        TestTlsAuthority.Identity createdCoordinator;
        int chosenPort;
        try {
            createdWarehouse = Files.createDirectory(workspace.resolve("warehouse"));
            createdSecurity = new ProcessSecurityAuthority(SharedTestContainers.NETWORK);
            createdReports = new ProcessStatusReportRelay();
            createdTls = new TestTlsAuthority(workspace);
            createdClient = createdTls.issue("local-client", "localhost", false);
            createdCoordinator = createdTls.issue("local-coordinator", advertisedAddress, false);
            workerIdentities.put("local-worker-1",
                    createdTls.issue("local-worker-1", advertisedAddress, false));
            workerIdentities.put("local-worker-2",
                    createdTls.issue("local-worker-2", advertisedAddress, false));
            chosenPort = availablePort();
        } catch (Exception | Error failure) {
            for (AutoCloseable owner : new AutoCloseable[] {
                    createdReports, createdSecurity, createdTls}) {
                if (owner == null) continue;
                try {
                    owner.close();
                } catch (Exception cleanup) {
                    failure.addSuppressed(cleanup);
                }
            }
            throw new IllegalStateException(
                    "Local process fixture construction failed; diagnostics retained at "
                            + workspace, failure);
        }
        warehouse = createdWarehouse;
        security = createdSecurity;
        reports = createdReports;
        tls = createdTls;
        clientIdentity = createdClient;
        coordinatorIdentity = createdCoordinator;
        coordinatorPort = chosenPort;
    }

    void startCoordinatorAndFirstWorker() throws Exception {
        security.start();
        coordinator = launch("coordinator",
                "org.pangrova.coordinator.bootstrap.PangrovaCoordinatorMain",
                coordinatorYaml(), "-Xms128m", "-Xmx512m",
                "-XX:MaxDirectMemorySize=128m",
                // CLI status polling and multi-file seeding exceed the public default of 100/min.
                // Rate-limit behavior is covered by dedicated HTTP integration tests.
                "-Dpangrova.http.max-requests-per-minute=100000",
                "-Dpangrova.speculation.single-task-duration-threshold-ms=15000");
        awaitCoordinatorControlPlane();
        reports.connect(URI.create("http://127.0.0.1:" + coordinatorPort));
        startWorker("local-worker-1");
        query("SELECT 42", ResultProtocol.GRPC_TLS);
    }

    void startSecondWorker() throws Exception {
        startWorker("local-worker-2");
        var first = acceptedWorkers.get("local-worker-1").request();
        var second = acceptedWorkers.get("local-worker-2").request();
        if (first.httpPort() == second.httpPort()
                || first.resultServerPort() == second.resultServerPort()) {
            throw new IllegalStateException("Two live Workers registered overlapping endpoints");
        }
    }

    void createSourceCatalog() throws Exception {
        String location = warehouse.toString().replace("'", "''");
        String jdbc = hostJdbcUrl().replace("'", "''");
        query("CREATE CATALOG spec_catalog WITH ('connector' = 'iceberg', "
                + "'type' = 'jdbc', 'uri' = '" + jdbc + "', "
                + "'jdbc.user' = 'pangrova', 'jdbc.password' = 'pangrova_test', "
                + "'jdbc.schema-version' = 'V1', 'warehouse' = '" + location + "')",
                ResultProtocol.GRPC_TLS);
        awaitClusterReady();
    }

    private void startWorker(String id) throws Exception {
        long after = System.nanoTime();
        Path spill = Files.createDirectories(workspace.resolve(id + "-spill"));
        Path shuffleSpill = Files.createDirectories(workspace.resolve(id + "-shuffle-spill"));
        OwnedProcess process = launch(id,
                "org.pangrova.worker.bootstrap.PangrovaWorkerMain",
                workerYaml(id, spill, shuffleSpill), "-Xms128m", "-Xmx384m",
                "-XX:MaxDirectMemorySize=128m", "-Dio.netty.maxDirectMemory=0");
        workers.put(id, process);
        RegisteredWorker accepted = awaitRegistration(id, after, process);
        WorkerRegistrationRequest registration = accepted.request();
        if (!advertisedAddress.getHostAddress().equals(registration.httpHost())
                || registration.httpPort() <= 0 || registration.resultServerPort() <= 0
                || accepted.coordinatorEpoch() <= 0) {
            throw new IllegalStateException("Worker registered an unroutable endpoint: " + id);
        }
        var inventory = CapabilityInventoryCodec.decode(registration.capabilityInventory());
        if (inventory.getCapabilitiesCount() == 0) {
            throw new IllegalStateException("Worker registered no native capabilities: " + id);
        }
        awaitHealth(id, process, registration.httpPort());
        awaitWorkerCapacity(id, accepted.receivedNanos(), process);
        acceptedWorkers.put(id, accepted);
    }

    void stopAndRestartFirstWorker() throws Exception {
        stopAndRestartFirstWorker(() -> {});
    }

    @FunctionalInterface
    interface WhileFirstWorkerStopped {
        void run() throws Exception;
    }

    void stopAndRestartFirstWorker(WhileFirstWorkerStopped whileStopped) throws Exception {
        OwnedProcess old = workers.get("local-worker-1");
        long oldPid = old.process().pid();
        RegisteredWorker previous = acceptedWorkers.get("local-worker-1");
        terminateGracefully(old, Duration.ofSeconds(60));
        workers.remove("local-worker-1");
        acceptedWorkers.remove("local-worker-1");
        query("SELECT 42", ResultProtocol.GRPC_TLS); // second Worker serves a public query alone
        whileStopped.run();
        startWorker("local-worker-1");
        if (workers.get("local-worker-1").process().pid() == oldPid) {
            throw new IllegalStateException("Restart reused the old Worker process");
        }
        RegisteredWorker replacement = acceptedWorkers.get("local-worker-1");
        if (replacement.receivedNanos() <= previous.receivedNanos()
                || replacement.coordinatorEpoch() < previous.coordinatorEpoch()) {
            throw new IllegalStateException("Restart did not publish a fresh, fenced registration");
        }
    }

    long pid(String workerId) { return workers.get(workerId).process().pid(); }
    long coordinatorPid() { return coordinator.process().pid(); }

    boolean hasAttributedQueryAudit(String queryId) throws IOException {
        String record = " msg=query=" + queryId + " user=" + ProcessSecurityAuthority.USER
                + " tenant=" + ProcessSecurityAuthority.TENANT + " ";
        try (var lines = Files.lines(workspace.resolve("logs/coordinator.log"), StandardCharsets.UTF_8)) {
            return lines.anyMatch(line -> line.contains("logger=pangrova.audit.query ")
                    && line.contains(record));
        }
    }
    Path retainDiagnostics() {
        preserveDiagnostics = true;
        return workspace;
    }
    ProcessStatusReportRelay reports() { return reports; }
    Path warehouse() { return warehouse; }
    String hostJdbcUrl() { return security.hostJdbcUrl(); }
    int coordinatorPort() { return coordinatorPort; }
    HttpResponse<byte[]> replayReport(ProcessStatusReportRelay.Delivery actual) throws Exception {
        var request = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + coordinatorPort
                        + "/v1/task/status-report"))
                .header("Authorization", "Bearer " + internalSecret)
                .header("Content-Type", "application/json").timeout(Duration.ofSeconds(30))
                .POST(HttpRequest.BodyPublishers.ofByteArray(actual.body())).build();
        return http.send(request, HttpResponse.BodyHandlers.ofByteArray());
    }
    void awaitClusterReady() {
        awaitHealth("Coordinator with registered Worker and catalog", coordinator, coordinatorPort);
    }
    int activeTenantReservations() {
        return new JdbcClusterConcurrencySlots(security.dataSource())
                .active(ProcessSecurityAuthority.TENANT);
    }

    QueryStatusResponse status(String queryId) {
        try (var client = PangrovaRestClient.builder()
                .coordinatorHost("127.0.0.1").coordinatorPort(coordinatorPort)
                .authentication(ClientAuthentication.basic(
                        ProcessSecurityAuthority.USER, ProcessSecurityAuthority.PASSWORD))
                .build()) {
            return client.getQueryStatus(queryId);
        }
    }

    QueryResult query(String sql, ResultProtocol expectedProtocol) throws Exception {
        return requireSuccessfulQuery(executeSql(sql, expectedProtocol), expectedProtocol);
    }

    QueryResult requireSuccessfulQuery(CliOutcome outcome, ResultProtocol expectedProtocol) {
        if (outcome.exitCode() != 0 || outcome.receipt() == null
                || outcome.output() == null) {
            preserveDiagnostics = true;
            throw new IllegalStateException("Public CLI query failed: exit=" + outcome.exitCode()
                    + " log=" + outcome.errors() + " workspace=" + workspace);
        }
        JsonNode record = outcome.receipt();
        if (!"FINISHED".equals(record.path("state").asText())
                || !record.path("resultConsumed").asBoolean()
                || record.path("queryId").asText().isBlank()) {
            preserveDiagnostics = true;
            throw new IllegalStateException("CLI did not prove terminal result handling: " + record);
        }
        String queryId = record.path("queryId").asText();
        QueryStatusResponse publicStatus = status(queryId);
        if (!"FINISHED".equals(publicStatus.state())
                || publicStatus.resultEndpoint() == null
                || !expectedProtocol.value().equals(publicStatus.resultEndpoint().protocol())) {
            preserveDiagnostics = true;
            throw new IllegalStateException("Public query state or typed result protocol disagrees"
                    + " with CLI receipt: " + queryId + " status=" + publicStatus);
        }
        return new QueryResult(queryId, outcome.output());
    }

    CliOutcome executeSql(String sql, ResultProtocol expectedProtocol) throws Exception {
        String suffix = UUID.randomUUID().toString();
        Path source = privateFile("query-" + suffix, ".sql");
        Path output = workspace.resolve("query-" + suffix + ".out");
        Path receipt = workspace.resolve("query-" + suffix + ".receipt");
        Path log = privateFile("query-" + suffix, ".log");
        Files.writeString(source, sql, StandardCharsets.UTF_8);
        List<String> command = new ArrayList<>(List.of(javaExecutable.toString(), "-Xmx256m",
                "-XX:MaxDirectMemorySize=128m",
                "--add-opens=java.base/java.nio=ALL-UNNAMED",
                "-Dpangrova.test.runtime-manifest=" + artifacts.manifestPath(),
                "-Dpangrova.test.runtime-manifest-sha256=" + artifacts.manifestSha256(),
                "-Dpangrova.test.native-sha256=" + artifacts.nativeSha256(),
                "-jar", artifacts.cliJar().toString(),
                "--host", "127.0.0.1", "--port", Integer.toString(coordinatorPort),
                "--user", ProcessSecurityAuthority.USER,
                "--password", ProcessSecurityAuthority.PASSWORD,
                "--query-timeout", "5m"));
        if (expectedProtocol == ResultProtocol.GRPC_TLS) {
            command.addAll(List.of("--result-tls-root-ca", tls.root().toString(),
                    "--result-tls-certificate-chain", clientIdentity.certificateChain().toString(),
                    "--result-tls-private-key", clientIdentity.privateKey().toString()));
        } else if (expectedProtocol != ResultProtocol.GRPC) {
            throw new IllegalArgumentException("Unsupported expected result protocol: " + expectedProtocol);
        }
        command.addAll(List.of("--file", source.toString(), "--output", output.toString(),
                "--execution-receipt", receipt.toString()));
        Process client = new ProcessBuilder(command).redirectErrorStream(true)
                .redirectOutput(log.toFile()).start();
        owned.add(new OwnedProcess("cli-" + suffix, client, source, log));
        LOG.info("event=local_process_spawn role=cli pid={} manifest={} manifestSha256={} "
                        + "nativeSha256={} advertisedAddress={}",
                client.pid(), artifacts.manifestPath(), artifacts.manifestSha256(),
                artifacts.nativeSha256(), advertisedAddress.getHostAddress());
        try {
            if (!client.waitFor(6, TimeUnit.MINUTES)) {
                stopClient(client, log);
                throw new IllegalStateException("CLI timed out: " + tail(log));
            }
        } catch (InterruptedException interrupted) {
            try {
                stopClient(client, log);
            } catch (Exception cleanup) {
                interrupted.addSuppressed(cleanup);
            }
            Thread.currentThread().interrupt();
            preserveDiagnostics = true;
            throw interrupted;
        }
        JsonNode record = Files.isRegularFile(receipt)
                ? JsonMapperFactory.shared().readTree(Files.readString(receipt)) : null;
        return new CliOutcome(client.exitValue(), Files.isRegularFile(output) ? Files.readString(output) : null,
                Files.readString(log), record);
    }

    private RegisteredWorker awaitRegistration(
            String workerId, long afterNanos, OwnedProcess process) {
        Awaitility.await("real registration " + workerId)
                .pollDelay(Duration.ZERO).pollInterval(Duration.ofMillis(200))
                .atMost(STARTUP_TIMEOUT).until(() -> {
                    requireAlive(process);
                    return reports.registrations().stream()
                            .anyMatch(reg -> reg.receivedNanos() >= afterNanos
                                    && workerId.equals(reg.request().workerId())
                                    && reg.response().get() != null
                                    && reg.response().get().statusCode() == 200);
                });
        var delivery = reports.registrations().stream()
                .filter(reg -> reg.receivedNanos() >= afterNanos
                        && workerId.equals(reg.request().workerId())
                        && reg.response().get() != null
                        && reg.response().get().statusCode() == 200)
                .reduce((left, right) -> right).orElseThrow();
        try {
            WorkerControlResponse response = JsonMapperFactory.shared().readValue(
                    delivery.response().get().body(), WorkerControlResponse.class);
            response.requireStatus("registered");
            return new RegisteredWorker(delivery.request(),
                    response.requireCoordinatorEpoch(), delivery.receivedNanos());
        } catch (IOException failure) {
            throw new IllegalStateException("Accepted Worker registration lacks canonical ACK", failure);
        }
    }

    private void awaitCoordinatorControlPlane() {
        Awaitility.await("Coordinator control plane before Worker/catalog bootstrap")
                .pollDelay(Duration.ZERO).pollInterval(Duration.ofMillis(200))
                .atMost(STARTUP_TIMEOUT).until(() -> {
                    requireAlive(coordinator);
                    try {
                        var request = HttpRequest.newBuilder(URI.create(
                                "http://127.0.0.1:" + coordinatorPort + "/v1/health"))
                                .timeout(Duration.ofSeconds(2)).GET().build();
                        var response = http.send(request, HttpResponse.BodyHandlers.ofString());
                        if (response.statusCode() != 200 && response.statusCode() != 503) return false;
                        JsonNode health = JsonMapperFactory.shared().readTree(response.body());
                        return "COORDINATOR".equals(health.path("nodeType").asText())
                                && health.path("checks").path("http").asBoolean()
                                && health.path("checks").path("transport").asBoolean();
                    } catch (IOException ignored) {
                        return false;
                    }
                });
    }

    private void awaitWorkerCapacity(String id, long registrationNanos, OwnedProcess process) {
        Awaitility.await("accepted actual resource heartbeat for " + id)
                .pollDelay(Duration.ZERO).pollInterval(Duration.ofMillis(200))
                .atMost(STARTUP_TIMEOUT).until(() -> {
                    requireAlive(process);
                    return reports.heartbeats().stream().anyMatch(heartbeat ->
                            heartbeat.receivedNanos() >= registrationNanos
                                    && id.equals(heartbeat.request().workerId())
                                    && heartbeat.request().availableMemoryBytes() > 0
                                    && "NONE".equals(heartbeat.request().memoryPressure())
                                    && heartbeat.response().get() != null
                                    && heartbeat.response().get().statusCode() == 200);
                });
    }

    private void awaitHealth(String role, OwnedProcess process, int port) {
        Awaitility.await(role + " health")
                .pollDelay(Duration.ZERO).pollInterval(Duration.ofMillis(200))
                .atMost(STARTUP_TIMEOUT).until(() -> {
                    requireAlive(process);
                    try {
                        var request = HttpRequest.newBuilder(URI.create(
                                "http://127.0.0.1:" + port + "/v1/health"))
                                .timeout(Duration.ofSeconds(2)).GET().build();
                        return http.send(request, HttpResponse.BodyHandlers.discarding())
                                .statusCode() == 200;
                    } catch (IOException ignored) {
                        return false;
                    }
                });
    }

    private static void requireAlive(OwnedProcess process) {
        if (!process.process().isAlive()) {
            throw new IllegalStateException(process.name() + " exited: " + tail(process.log()));
        }
    }

    private OwnedProcess launch(String name, String mainClass, String yaml, String... jvmArgs)
            throws IOException {
        Path config = privateFile(name, ".yml");
        Path log = privateFile(name, ".log");
        Files.writeString(config, yaml, StandardCharsets.UTF_8);
        List<String> command = new ArrayList<>();
        command.add(javaExecutable.toString());
        command.addAll(Arrays.asList(jvmArgs));
        Path loggingConfig = TestProjectPaths.findProjectRoot()
                .resolve("etc/logback.xml").toRealPath();
        Path logDirectory = Files.createDirectories(workspace.resolve("logs"));
        command.add("-Dlogback.configurationFile=" + loggingConfig);
        command.add("-DPANGROVA_LOG_DIR=" + logDirectory);
        command.add("-DPANGROVA_ROLE=" + name);
        command.add("-Dpangrova.test.runtime-manifest=" + artifacts.manifestPath());
        command.add("-Dpangrova.test.runtime-manifest-sha256=" + artifacts.manifestSha256());
        command.add("-Dpangrova.test.native-sha256=" + artifacts.nativeSha256());
        command.addAll(List.of("--add-opens=java.base/java.nio=ALL-UNNAMED",
                "--add-opens=java.base/java.lang=ALL-UNNAMED",
                "--add-opens=java.base/java.util=ALL-UNNAMED",
                "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
                "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
                "-cp", artifacts.classpath(), mainClass, "--config", config.toString()));
        Process process = new ProcessBuilder(command)
                .redirectErrorStream(true).redirectOutput(log.toFile()).start();
        OwnedProcess owner = new OwnedProcess(name, process, config, log);
        owned.add(owner);
        LOG.info("event=local_process_spawn role={} pid={} manifest={} manifestSha256={} "
                        + "nativeSha256={} advertisedAddress={}",
                name, process.pid(), artifacts.manifestPath(), artifacts.manifestSha256(),
                artifacts.nativeSha256(), advertisedAddress.getHostAddress());
        if (process.pid() == ProcessHandle.current().pid()) {
            throw new IllegalStateException("Production process was not isolated: " + name);
        }
        return owner;
    }

    private String coordinatorYaml() {
        return """
                coordinator:
                  id: local-process-coordinator
                  host: 127.0.0.1
                  port: %d
                  role: QUERY
                  internal-auth-secret: "%s"
                  speculation:
                    enabled: %s
                  ha:
                    enabled: false
                  transport:
                    advertised-host: "%s"
                    allocator-limit-bytes: 16777216
                    tls:
                      enabled: true
                      certificate-chain-path: "%s"
                      private-key-path: "%s"
                      root-ca-path: "%s"
                  shuffle:
                    tls:
                      server-identity: "%s"
                state-store:
                  type: JDBC
                metadata:
                  enabled: true
                  jdbc-url: "%s"
                  jdbc-username: pangrova
                  jdbc-password: pangrova_test
                security:
                  enabled: true
                  auth:
                    type: LDAP
                    properties:
                      ldap-url: "%s"
                      base-dn: dc=pangrova,dc=org
                      user-dn-pattern: uid={0},dc=pangrova,dc=org
                tenant:
                  enabled: true
                """.formatted(coordinatorPort, internalSecret, speculation, advertisedAddress.getHostAddress(),
                coordinatorIdentity.certificateChain(), coordinatorIdentity.privateKey(),
                tls.root(), advertisedAddress.getHostAddress(),
                security.hostJdbcUrl(), security.hostLdapUrl());
    }

    private String workerYaml(String id, Path spill, Path shuffleSpill) throws IOException {
        var identity = workerIdentities.get(id);
        return """
                worker:
                  admission:
                    state-directory: "%s"
                  id: "%s"
                  host: 0.0.0.0
                  advertised-host: "%s"
                  port: 0
                  coordinator-url: "http://127.0.0.1:%d"
                  internal-auth-secret: "%s"
                  transport:
                    port: 0
                    tls:
                      enabled: true
                      certificate-chain-path: "%s"
                      private-key-path: "%s"
                      root-ca-path: "%s"
                      identity: "%s"
                  memory:
                    budget-bytes: 536870912
                    system-reserved-bytes: 67108864
                    node-bytes: 1073741824
                  threads:
                    query: 2
                    io: 2
                    scheduler: 1
                  spill:
                    directory: "%s"
                  shuffle:
                    service:
                      retention-bytes: 16777216
                      transit-bytes: 16777216
                      spill-bytes: 268435456
                      spill-files: 64
                      spill-directory: "%s"
                  native:
                    cache:
                      enabled: false
                  cdc:
                    enabled: false
                """.formatted(workspace.resolve("admission-" + id), id, advertisedAddress.getHostAddress(), reports.port(), internalSecret,
                identity.certificateChain(), identity.privateKey(), tls.root(),
                advertisedAddress.getHostAddress(),
                spill, shuffleSpill) + aiWorkerYaml();
    }

    private String aiWorkerYaml() throws IOException {
        var yaml = new StringBuilder();
        for (var setting : functions.configForPlugin("ai-inference").entrySet()) {
            if (!setting.getKey().matches("[a-z0-9.-]+")) {
                throw new IllegalArgumentException("Invalid AI deployment configuration key");
            }
            yaml.append("worker.ai.").append(setting.getKey()).append(": ")
                    .append(JsonMapperFactory.shared().writeValueAsString(setting.getValue())).append('\n');
        }
        return yaml.toString();
    }

    private Path privateFile(String prefix, String suffix) throws IOException {
        return Files.createTempFile(workspace, prefix + "-", suffix,
                PosixFilePermissions.asFileAttribute(
                        PosixFilePermissions.fromString("rw-------")));
    }

    private static int availablePort() throws IOException {
        try (var socket = new ServerSocket()) {
            socket.bind(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 0));
            return socket.getLocalPort();
        }
    }

    private static Inet4Address routableHostAddress() throws IOException {
        var interfaces = NetworkInterface.getNetworkInterfaces();
        while (interfaces.hasMoreElements()) {
            var network = interfaces.nextElement();
            if (!network.isUp() || network.isLoopback() || network.isVirtual()) continue;
            var addresses = network.getInetAddresses();
            while (addresses.hasMoreElements()) {
                var address = addresses.nextElement();
                if (address instanceof Inet4Address ipv4 && !ipv4.isLoopbackAddress()
                        && !ipv4.isLinkLocalAddress() && !ipv4.isAnyLocalAddress()) {
                    return ipv4;
                }
            }
        }
        throw new IllegalStateException("No routable local IPv4 address for Worker registration");
    }

    private static String tail(Path path) {
        try (var channel = Files.newByteChannel(path)) {
            long size = channel.size();
            channel.position(Math.max(0, size - 20_000));
            var bytes = java.nio.ByteBuffer.allocate((int) Math.min(size, 20_000));
            while (bytes.hasRemaining() && channel.read(bytes) != -1) {
                // Drain the bounded log tail until the buffer is full or EOF.
            }
            bytes.flip();
            return StandardCharsets.UTF_8.decode(bytes).toString();
        } catch (IOException failure) {
            return failure.toString();
        }
    }

    private static void terminateGracefully(OwnedProcess owner, Duration timeout) {
        boolean[] interrupted = {Thread.interrupted()};
        try {
            if (!owner.process().isAlive()) {
                throw new IllegalStateException(owner.name() + " exited before SIGTERM: "
                        + tail(owner.log()));
            }
            owner.process().destroy();
            if (!awaitExit(owner.process(), timeout, interrupted)) {
                throw new IllegalStateException(owner.name() + " did not drain after SIGTERM: "
                        + tail(owner.log()));
            }
            String log = tail(owner.log());
            if (!log.contains("event=worker_shutdown_completed")
                    || !log.contains("event=worker_deregistration_ack workerId="
                            + owner.name())) {
                throw new IllegalStateException(owner.name()
                        + " exited without drain and deregistration receipts: " + log);
            }
        } finally {
            if (interrupted[0]) Thread.currentThread().interrupt();
        }
    }

    private static void stopClient(Process client, Path log) {
        boolean[] interrupted = {Thread.interrupted()};
        try {
            if (!client.isAlive()) return;
            client.destroy();
            if (awaitExit(client, Duration.ofSeconds(3), interrupted)) return;
            client.destroyForcibly();
            LOG.warn("event=local_cli_force_kill pid={} log={}", client.pid(), log);
            if (!awaitExit(client, Duration.ofSeconds(10), interrupted)) {
                throw new IllegalStateException("CLI child remains alive after bounded force-kill: "
                        + tail(log));
            }
        } finally {
            if (interrupted[0]) Thread.currentThread().interrupt();
        }
    }

    private static boolean awaitExit(Process process, Duration timeout, boolean[] interrupted) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (process.isAlive()) {
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) return false;
            try {
                process.waitFor(remaining, TimeUnit.NANOSECONDS);
            } catch (InterruptedException ignored) {
                interrupted[0] = true;
            }
        }
        return true;
    }

    private Exception stopAllOwned(boolean[] interrupted) {
        Exception failure = null;
        for (int index = owned.size() - 1; index >= 0; index--) {
            OwnedProcess owner = owned.get(index);
            try {
                if (owner.process().isAlive()) {
                    owner.process().destroy();
                    if (!awaitExit(owner.process(), Duration.ofSeconds(30), interrupted)) {
                        owner.process().destroyForcibly();
                        LOG.warn("event=local_process_force_kill role={} pid={} log={}",
                                owner.name(), owner.process().pid(), owner.log());
                        if (!awaitExit(owner.process(), Duration.ofSeconds(10), interrupted)) {
                            throw new IllegalStateException(owner.name()
                                    + " is still live after bounded force-kill");
                        }
                        throw new IllegalStateException(owner.name() + " required SIGKILL");
                    }
                }
            } catch (Exception cleanup) {
                if (failure == null) {
                    failure = cleanup;
                } else {
                    failure.addSuppressed(cleanup);
                }
            }
        }
        return failure;
    }

    @Override public void close() throws Exception {
        if (closed) return;
        boolean[] interrupted = {Thread.interrupted()};
        try {
            closeOwnersAndAuthorities(interrupted);
        } finally {
            if (interrupted[0]) Thread.currentThread().interrupt();
        }
    }

    private void closeOwnersAndAuthorities(boolean[] interrupted) throws Exception {
        Exception failure = stopAllOwned(interrupted);
        if (owned.stream().anyMatch(owner -> owner.process().isAlive())) {
            preserveDiagnostics = true;
            throw new IllegalStateException(
                    "Owned child JVM is still live; authority and relay retained for retry; "
                            + "diagnostics=" + workspace,
                    failure);
        }
        if (!reportsClosed) {
            try {
                reports.close();
                reportsClosed = true;
            } catch (Exception cleanup) {
                if (failure == null) {
                    failure = cleanup;
                } else {
                    failure.addSuppressed(cleanup);
                }
                interrupted[0] |= Thread.interrupted();
            }
        }
        if (reportsClosed && !securityClosed) {
            try {
                security.close();
                securityClosed = true;
            } catch (Exception cleanup) {
                if (failure == null) {
                    failure = cleanup;
                } else {
                    failure.addSuppressed(cleanup);
                }
            }
        }
        if (reportsClosed && securityClosed && !tlsClosed) {
            try {
                tls.close();
                tlsClosed = true;
            } catch (Exception cleanup) {
                if (failure == null) {
                    failure = cleanup;
                } else {
                    failure.addSuppressed(cleanup);
                }
            }
        }
        if (failure == null && !preserveDiagnostics) {
            try {
                deleteWorkspace(workspace);
            } catch (IOException cleanup) {
                failure = cleanup;
            }
        }
        if (failure != null) preserveDiagnostics = true;
        else closed = true;
        if (failure != null) {
            failure.addSuppressed(new IllegalStateException(
                    "Local process diagnostics retained at " + workspace));
            throw failure;
        }
    }

    private static void deleteWorkspace(Path root) throws IOException {
        try (var paths = Files.walk(root)) {
            for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) {
                Files.delete(path);
            }
        }
    }

    record QueryResult(String queryId, String output) { }

    record CliOutcome(int exitCode, String output, String errors, JsonNode receipt) { }

    private record OwnedProcess(String name, Process process, Path config, Path log) { }

    private record RegisteredWorker(
            WorkerRegistrationRequest request, long coordinatorEpoch, long receivedNanos) { }
}

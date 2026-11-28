package org.pangrova.testing.integration.e2e.speculation;

import com.fasterxml.jackson.databind.JsonNode;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.client.http.PangrovaRestClient;
import org.pangrova.client.http.authentication.ClientAuthentication;
import org.pangrova.common.client.enums.ResultProtocol;
import org.pangrova.common.client.query.QueryStatusResponse;
import org.pangrova.common.client.result.ResultEndpoint;
import org.pangrova.common.enums.QueryWaitTarget;
import org.pangrova.common.enums.ResultDisposition;
import org.pangrova.common.json.JsonMapperFactory;
import org.pangrova.protocol.v2.TaskReport;
import org.pangrova.protocol.v2.TaskState;
import org.pangrova.testing.testkit.ContainerTestBase;
import org.pangrova.testing.testkit.E2ETest;
import org.pangrova.testing.testkit.PangrovaClusterContainer;
import org.pangrova.testing.testkit.security.TestTlsAuthority;
import org.pangrova.transport.client.security.ResultTransportSecurity;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.Testcontainers;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** Real service containers, with a durable journal and pass-through observation of actual task reports. */
@E2ETest
@Tag("chaos")
class CoordinatorRestartChaosTest extends ContainerTestBase {
    private static final String WORKER = "e2e-worker-1";
    private static final String WORKER_HOST = "worker-1";
    private static final int WORKER_HTTP_PORT = 8081;
    private static final int WORKER_RESULT_PORT = 9090;
    private static final String WORKER_CONTROL_URL = "http://chaos-proxy:9090";
    private static final String CONFIG = "/opt/pangrova/config/";
    private static final Duration TIMEOUT = Duration.ofMinutes(3);

    @Test
    void journalAndExecutionRemainObservableAcrossProcessAndNetworkFailures(@TempDir Path workspace) throws Exception {
        String image = required("pangrova.e2e.image");
        String imageId = required("pangrova.e2e.image-id");
        String commit = required("pangrova.e2e.local.source-commit");
        assertThat(commit).matches("[0-9a-f]{40}");
        var dockerFactory = DockerClientFactory.instance();
        var docker = dockerFactory.client();
        var mappedResultAddress = InetAddress.getByName(dockerFactory.dockerHostIpAddress());
        var inspected = docker.inspectImageCmd(image).exec();
        assertThat(inspected.getId()).isEqualTo(imageId);
        assertThat(inspected.getConfig().getLabels()).containsEntry("org.opencontainers.image.revision", commit);

        try (var network = Network.newNetwork();
             var security = new ProcessSecurityAuthority(network);
             var reports = new ProcessStatusReportRelay();
             var tls = new TestTlsAuthority(workspace);
             var proxy = new ToxiproxyContainer(DockerImageName.parse("ghcr.io/shopify/toxiproxy:2.12.0"))
                     .withNetwork(network).withNetworkAliases("chaos-proxy");
             var cluster = new PangrovaClusterContainer(DockerImageName.parse(image), 1, network)) {
            security.start();
            Testcontainers.exposeHostPorts(security.postgresPort(), reports.port());
            proxy.start();
            var link = new ToxiproxyClient(proxy.getHost(), proxy.getControlPort()).createProxy("worker-control",
                    "0.0.0.0:9090", "host.testcontainers.internal:" + reports.port());
            var coordinatorIdentity = tls.issue("coordinator", "coordinator", false);
            var workerIdentity = tls.issueForMappedEndpoint(WORKER_HOST, WORKER_HOST, mappedResultAddress);
            var clientIdentity = tls.issue("result-client", "result-client", false);
            String secret = UUID.randomUUID().toString();
            cluster.configureCoordinator(container -> configure(container, "coordinator",
                    coordinatorConfig(security.processJdbcUrl(), secret), tls, coordinatorIdentity));
            cluster.configureWorkers((number, container) -> {
                var coordinator = cluster.getCoordinator();
                reports.connect(URI.create("http://" + coordinator.getHost() + ":" + coordinator.getMappedPort(8080)));
                container.withEnv("PANGROVA_COORDINATOR_URL", WORKER_CONTROL_URL);
                container.addExposedPort(WORKER_RESULT_PORT);
                configure(container, "worker", workerConfig(secret), tls, workerIdentity);
            });
            cluster.start();
            var coordinator = cluster.getCoordinator();
            var worker = cluster.getWorkers().get(0);
            var mappedResult = new InetSocketAddress(mappedResultAddress, worker.getMappedPort(WORKER_RESULT_PORT));
            assertThat(coordinator.getContainerInfo().getImageId()).isEqualTo(imageId);
            assertThat(worker.getContainerInfo().getImageId()).isEqualTo(imageId);
            var auth = ClientAuthentication.basic(ProcessSecurityAuthority.USER, ProcessSecurityAuthority.PASSWORD);
            var workersRequest = HttpRequest.newBuilder(URI.create(cluster.getCoordinatorUrl() + "/api/workers"))
                    .header("Authorization", auth.authorizationHeader()).timeout(Duration.ofSeconds(5)).GET().build();
            var observer = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
            try (var client = PangrovaRestClient.builder().coordinatorHost(coordinator.getHost())
                    .coordinatorPort(coordinator.getMappedPort(8080)).authentication(auth)
                    .resultTransportSecurity(new ResultTransportSecurity(
                            true, tls.root(), clientIdentity.certificateChain(), clientIdentity.privateKey()))
                    .build()) {
                awaitWorker(observer, workersRequest, true);
                String completed = nativeQuery(client, mappedResult, reports, security);
                link.disable();
                try {
                    awaitWorker(observer, workersRequest, false);
                } finally {
                    link.enable();
                }
                awaitWorker(observer, workersRequest, true);

                // Hold the actual terminal task report before its delivery. The coordinator must still own RUNNING.
                try (var held = reports.holdReports(report -> "FINISHED".equals(report.state()))) {
                    String active = drainNativeQuery(client, mappedResult);
                    awaitNativeReport(reports, active, false);
                    await("active query before Worker loss").atMost(TIMEOUT)
                            .untilAsserted(() -> assertState(client, active, "RUNNING"));
                    docker.killContainerCmd(worker.getContainerId()).withSignal("KILL").exec();
                    awaitWorker(observer, workersRequest, false);
                    await("query fails after losing its only execution Worker").atMost(TIMEOUT).untilAsserted(() -> {
                        assertState(client, active, "FAILED");
                        assertThat(client.getQueryStatus(active).error()).isNotNull();
                        assertThat(journalState(security, active)).isEqualTo("FAILED");
                    });
                }
                docker.startContainerCmd(worker.getContainerId()).exec();
                awaitWorker(observer, workersRequest, true);
                awaitWorkerRoute(coordinator);
                nativeQuery(client, mappedResult, reports, security);

                try (var held = reports.holdReports(report -> "FINISHED".equals(report.state()))) {
                    String active = drainNativeQuery(client, mappedResult);
                    awaitNativeReport(reports, active, false);
                    await("RUNNING is durable before Coordinator loss").atMost(TIMEOUT).untilAsserted(() -> {
                        assertState(client, active, "RUNNING");
                        assertThat(journalState(security, active)).isEqualTo("RUNNING");
                    });
                    restartCoordinator(coordinator, observer, URI.create(cluster.getCoordinatorUrl()));
                    assertState(client, completed, "FINISHED");
                    assertThat(journalState(security, completed)).isEqualTo("FINISHED");
                    // A non-HA restart serves the last journal fact even without the old in-memory task.
                    assertState(client, active, "RUNNING");
                    assertThat(journalState(security, active)).isEqualTo("RUNNING");
                }
                awaitWorker(observer, workersRequest, true);
                nativeQuery(client, mappedResult, reports, security);
            }
        }
    }

    private static void restartCoordinator(GenericContainer<?> process, HttpClient http, URI base) {
        var docker = process.getDockerClient();
        String id = process.getContainerId();
        String previousStart = docker.inspectContainerCmd(id).exec().getState().getStartedAt();
        docker.killContainerCmd(id).withSignal("KILL").exec();
        docker.startContainerCmd(id).exec();
        var health = HttpRequest.newBuilder(base.resolve("/v1/health")).timeout(Duration.ofSeconds(3)).GET().build();
        await("new Coordinator process serves the control plane").atMost(TIMEOUT).until(() -> {
            var state = docker.inspectContainerCmd(id).exec().getState();
            assertThat(state.getRunning()).isTrue();
            assertThat(state.getStartedAt()).isNotEqualTo(previousStart);
            HttpResponse<String> response;
            try {
                response = http.send(health, HttpResponse.BodyHandlers.ofString());
            } catch (java.io.IOException starting) {
                return false;
            }
            assertThat(response.statusCode()).as(response.body()).isIn(200, 503);
            JsonNode body = JsonMapperFactory.shared().readTree(response.body());
            assertThat(body.path("nodeType").asText()).isEqualTo("COORDINATOR");
            return body.path("checks").path("http").asBoolean()
                    && body.path("checks").path("transport").asBoolean();
        });
    }

    private static String nativeQuery(PangrovaRestClient client, InetSocketAddress mappedResult,
            ProcessStatusReportRelay reports, ProcessSecurityAuthority security) {
        String id = drainNativeQuery(client, mappedResult);
        awaitNativeReport(reports, id, true);
        await("committed query " + id).atMost(TIMEOUT).untilAsserted(() -> {
            assertState(client, id, "FINISHED");
            assertThat(journalState(security, id)).isEqualTo("FINISHED");
        });
        return id;
    }

    private static String drainNativeQuery(PangrovaRestClient client, InetSocketAddress mappedResult) {
        long deadline = System.currentTimeMillis() + TIMEOUT.toMillis();
        var query = client.beginQuery("SELECT 21 + 21", Map.of("catalog", "spec_catalog"));
        String id = query.submission().queryId();
        assertThat(id).isNotBlank();
        assertThat(UUID.fromString(id).toString()).isEqualTo(id);
        QueryStatusResponse status;
        do {
            status = query.awaitStatus(QueryWaitTarget.RESULT_OR_TERMINAL, deadline);
            assertThat(status.error()).as("query %s before reading its result", id).isNull();
            assertThat(status.state()).as("query %s before reading its result", id)
                    .isNotIn("FAILED", "CANCELLED");
        } while (status.resultEndpoint() == null && !"FINISHED".equals(status.state()));
        assertThat(status.resultDisposition()).isEqualTo(ResultDisposition.STREAM);
        var endpoint = status.resultEndpoint();
        assertThat(endpoint).isNotNull();
        assertThat(endpoint.host()).isEqualTo(WORKER_HOST);
        assertThat(endpoint.port()).isEqualTo(WORKER_RESULT_PORT);
        assertThat(endpoint.protocol()).isEqualTo(ResultProtocol.GRPC_TLS.value());
        var mappedEndpoint = new ResultEndpoint(mappedResult.getAddress().getHostAddress(), mappedResult.getPort(),
                endpoint.ticket(), endpoint.protocol());
        // Native terminal evidence requires a committed Flight EOF, even when the status report is held.
        try (var cursor = client.openResult(mappedEndpoint, deadline)) {
            assertThat(cursor.next()).isTrue();
            assertThat(cursor.value(0)).isEqualTo(42);
            assertThat(cursor.next()).isFalse();
        }
        return id;
    }

    private static void assertState(PangrovaRestClient client, String id, String state) {
        var status = client.getQueryStatus(id);
        assertThat(status.queryId()).isEqualTo(id);
        assertThat(status.state()).isEqualTo(state);
    }

    private static void awaitNativeReport(ProcessStatusReportRelay relay, String id, boolean acknowledged) {
        await("exact Native task receipt for " + id).atMost(TIMEOUT).until(() -> {
            for (var observed : relay.deliveries()) {
                var report = observed.report();
                if (!id.equals(report.queryId()) || !WORKER.equals(report.workerId())
                        || !"FINISHED".equals(report.state()) || report.canonicalTaskReport() == null) continue;
                var nativeReport = TaskReport.parseFrom(report.canonicalTaskReport());
                assertThat(nativeReport.getAttempt().getTaskId()).isEqualTo(report.taskId());
                assertThat(nativeReport.getAttempt().getQueryOrJobId()).isEqualTo(id);
                assertThat(nativeReport.getState()).isEqualTo(TaskState.TASK_STATE_FINISHED);
                if (!acknowledged) return observed.response().get() == null;
                if (observed.response().get() != null && observed.response().get().statusCode() == 200) return true;
            }
            return false;
        });
    }

    private static String journalState(ProcessSecurityAuthority security, String id) throws Exception {
        try (var connection = security.dataSource().getConnection();
             var query = connection.prepareStatement(
                     "SELECT state FROM query_state_journal WHERE query_id = ? ORDER BY id DESC LIMIT 1")) {
            query.setString(1, id);
            try (var row = query.executeQuery()) {
                assertThat(row.next()).as("durable row for %s", id).isTrue();
                return row.getString(1);
            }
        }
    }

    private static void awaitWorker(HttpClient http, HttpRequest request, boolean active) {
        await("Worker " + WORKER + (active ? " has capacity" : " is dead or removed")).atMost(TIMEOUT)
                .pollInterval(Duration.ofSeconds(1)).untilAsserted(() -> {
                    var response = http.send(request, HttpResponse.BodyHandlers.ofString());
                    assertThat(response.statusCode()).as(response.body()).isEqualTo(200);
                    JsonNode list = JsonMapperFactory.shared().readTree(response.body());
                    assertThat(list.isArray()).as(response.body()).isTrue();
                    if (!active && list.isEmpty()) return;
                    assertThat(list.size()).as(response.body()).isOne();
                    var worker = list.get(0);
                    assertThat(worker.path("workerId").asText()).isEqualTo(WORKER);
                    assertThat(worker.path("healthState").asText()).isEqualTo(active ? "ACTIVE" : "DEAD");
                    if (active) assertThat(worker.path("availableMemoryBytes").asLong()).isPositive();
                });
    }

    private static void awaitWorkerRoute(GenericContainer<?> coordinator) throws Exception {
        // The Worker can register while the Coordinator JVM still caches a lookup failure from cancellation.
        var securityProperty = coordinator.execInContainer("sh", "-c",
                "sed -n 's/^networkaddress[.]cache[.]negative[.]ttl=//p' \"$JAVA_HOME/conf/security/java.security\"");
        assertThat(securityProperty.getExitCode()).as(securityProperty.getStderr()).isZero();
        long negativeCacheSeconds = Long.parseLong(securityProperty.getStdout().strip());
        assertThat(negativeCacheSeconds).isBetween(0L, TIMEOUT.toSeconds() - 1);
        String endpoint = "http://" + WORKER_HOST + ":" + WORKER_HTTP_PORT + "/v1/health";
        await("Worker is reachable from Coordinator through the negative DNS cache lifetime")
                .during(Duration.ofSeconds(negativeCacheSeconds)).atMost(TIMEOUT)
                .pollInterval(Duration.ofSeconds(1)).untilAsserted(() -> {
                    var health = coordinator.execInContainer("curl", "--fail", "--silent", "--show-error",
                            "--max-time", "2", endpoint);
                    assertThat(health.getExitCode()).as(health.getStderr()).isZero();
                    assertThat(JsonMapperFactory.shared().readTree(health.getStdout()).path("nodeType").asText())
                            .isEqualTo("WORKER");
                });
    }

    private static void configure(GenericContainer<?> container, String role, String yaml,
            TestTlsAuthority tls, TestTlsAuthority.Identity identity) {
        String configFile = CONFIG + "chaos-" + role + ".yml";
        container.withCommand("start", role, "--config", configFile)
                .withEnv("PANGROVA_JAVA_OPTS", "-Xms128m -Xmx512m -XX:MaxDirectMemorySize=128m"
                        + " -Dpangrova.http.max-requests-per-minute=10000")
                .withCopyToContainer(Transferable.of(yaml, 0400), configFile)
                .withCopyToContainer(MountableFile.forHostPath(tls.root(), 0444), CONFIG + "root.pem")
                .withCopyToContainer(MountableFile.forHostPath(identity.certificateChain(), 0444), CONFIG + "role.pem")
                .withCopyToContainer(MountableFile.forHostPath(identity.privateKey(), 0400), CONFIG + "role-key.pem");
    }

    private static String coordinatorConfig(String jdbc, String secret) {
        return """
                coordinator:
                  id: e2e-coordinator
                  host: 0.0.0.0
                  port: 8080
                  role: QUERY
                  internal-auth-secret: "%s"
                  ha:
                    enabled: false
                  transport:
                    advertised-host: coordinator
                    tls:
                      enabled: true
                      root-ca-path: /opt/pangrova/config/root.pem
                      certificate-chain-path: /opt/pangrova/config/role.pem
                      private-key-path: /opt/pangrova/config/role-key.pem
                  shuffle:
                    tls:
                      server-identity: coordinator
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
                      ldap-url: ldap://spec-directory:389
                      base-dn: dc=pangrova,dc=org
                      user-dn-pattern: uid={0},dc=pangrova,dc=org
                tenant:
                  enabled: true
                """.formatted(secret, jdbc);
    }

    private static String workerConfig(String secret) {
        return """
                worker:
                  id: e2e-worker-1
                  host: 0.0.0.0
                  advertised-host: %s
                  port: %d
                  coordinator-url: "%s"
                  internal-auth-secret: "%s"
                  heartbeat-interval: 2s
                  transport:
                    port: %d
                    tls:
                      enabled: true
                      identity: %s
                      root-ca-path: /opt/pangrova/config/root.pem
                      certificate-chain-path: /opt/pangrova/config/role.pem
                      private-key-path: /opt/pangrova/config/role-key.pem
                  memory:
                    budget-bytes: 536870912
                    node-bytes: 1073741824
                  shuffle:
                    service:
                      spill-directory: /opt/pangrova/data
                  native:
                    cache:
                      enabled: false
                """.formatted(WORKER_HOST, WORKER_HTTP_PORT, WORKER_CONTROL_URL, secret, WORKER_RESULT_PORT, WORKER_HOST);
    }

    private static String required(String property) {
        String value = System.getProperty(property);
        if (value == null || value.isBlank()) throw new IllegalArgumentException("Missing " + property);
        return value;
    }
}

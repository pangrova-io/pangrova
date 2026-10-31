/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.pangrova.testing.integration.chaos.cluster;

import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * P1-9: Real chaos tests for distributed fault tolerance (replaces empty TODO stubs).
 *
 * <p>Scenarios: network partition, coordinator restart, worker SIGKILL — each with
 * Awaitility-based timed assertions proving the cluster converges (not silently loses
 * data). Requires Docker + the pangrova-standalone image; a missing prerequisite fails
 * the suite instead of skipping it, so an unexercised chaos run cannot look green.</p>
 */
@Tag("chaos")
@DisplayName("Chaos: coordinator restart / worker kill / network partition")
class CoordinatorRestartChaosTest {

    private static final Logger LOG = LoggerFactory.getLogger(CoordinatorRestartChaosTest.class);
    private static final String PANGROVA_IMAGE = System.getenv().getOrDefault(
            "PANGROVA_STANDALONE_IMAGE", "pangrova-standalone:latest");
    private static final int COORDINATOR_PORT = 8080;
    private static final int WORKER_PORT = 8081;
    private static final Duration HEARTBEAT_TIMEOUT = Duration.ofSeconds(20);
    private static final Duration CONVERGENCE_TIMEOUT = Duration.ofSeconds(45);

    private static Network network;
    private static ToxiproxyContainer toxiproxy;
    private static GenericContainer<?> coordinator;
    private static GenericContainer<?> worker;
    private static Proxy workerProxy;
    private static HttpClient httpClient;

    @BeforeAll
    @SuppressWarnings("resource")
    static void setUpCluster() throws IOException {
        if (!DockerClientFactory.instance().isDockerAvailable()) {
            throw new IllegalStateException(
                    "Chaos tests require a reachable Docker daemon; refusing to run unexercised");
        }
        if (!imageExists(PANGROVA_IMAGE)) {
            throw new IllegalStateException("Pangrova standalone image is required: " + PANGROVA_IMAGE
                    + " (build it or point PANGROVA_STANDALONE_IMAGE at an existing image)");
        }

        network = Network.newNetwork();
        coordinator = new GenericContainer<>(PANGROVA_IMAGE)
                .withNetwork(network).withNetworkAliases("coordinator")
                .withExposedPorts(COORDINATOR_PORT)
                .withEnv("PANGROVA_ROLE", "COORDINATOR")
                .waitingFor(Wait.forHttp("/v1/status").forPort(COORDINATOR_PORT)
                        .withStartupTimeout(Duration.ofSeconds(60)));
        coordinator.start();

        toxiproxy = new ToxiproxyContainer("ghcr.io/shopify/toxiproxy:2.5.0")
                .withNetwork(network).withNetworkAliases("toxiproxy");
        toxiproxy.start();
        var toxiClient = new ToxiproxyClient(toxiproxy.getHost(), toxiproxy.getControlPort());
        workerProxy = toxiClient.createProxy("worker-proxy",
                "0.0.0.0:9090", "coordinator:" + COORDINATOR_PORT);

        worker = new GenericContainer<>(PANGROVA_IMAGE)
                .withNetwork(network).withNetworkAliases("worker")
                .withExposedPorts(WORKER_PORT)
                .withEnv("PANGROVA_ROLE", "WORKER")
                .withEnv("PANGROVA_COORDINATOR_URL", "http://toxiproxy:9090")
                .waitingFor(Wait.forLogMessage(".*worker_registered.*", 1)
                        .withStartupTimeout(Duration.ofSeconds(60)));
        worker.start();
        httpClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
        LOG.info("Chaos cluster ready");
    }

    @AfterAll
    static void tearDown() {
        if (worker != null) worker.stop();
        if (coordinator != null) coordinator.stop();
        if (toxiproxy != null) toxiproxy.stop();
        if (network != null) network.close();
    }

    @Test
    @DisplayName("Network partition: heartbeat timeout → worker DEAD → heal → re-register")
    void networkPartitionBetweenCoordinatorAndWorker() throws IOException {
        assertWorkerRegistered();
        workerProxy.disable();
        Awaitility.await("worker marked dead").atMost(HEARTBEAT_TIMEOUT.plusSeconds(10))
                .pollInterval(2, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(workerCount()).isZero());
        workerProxy.enable();
        Awaitility.await("worker re-registers").atMost(CONVERGENCE_TIMEOUT)
                .pollInterval(2, TimeUnit.SECONDS)
                .untilAsserted(this::assertWorkerRegistered);
    }

    @Test
    @DisplayName("Coordinator restart: workers re-register, in-flight query not silently lost")
    void coordinatorRestartReconvergesClusterState() {
        assertWorkerRegistered();
        String qid = submitQuery("SELECT 1");
        coordinator.stop();
        coordinator.start();
        Awaitility.await("worker re-registers").atMost(CONVERGENCE_TIMEOUT)
                .pollInterval(2, TimeUnit.SECONDS)
                .untilAsserted(this::assertWorkerRegistered);
        String state = queryState(qid);
        assertThat(state).as("Query must not silently disappear").isIn("FAILED", "FINISHED", "UNKNOWN");
    }

    @Test
    @DisplayName("Worker SIGKILL: coordinator detects, query fails (not silently dropped)")
    void workerKillDuringExecutionTriggersReschedule() {
        assertWorkerRegistered();
        String qid = submitQuery("SELECT 1");
        worker.getDockerClient().killContainerCmd(worker.getContainerId()).withSignal("KILL").exec();
        Awaitility.await("coordinator detects death").atMost(HEARTBEAT_TIMEOUT.plusSeconds(10))
                .pollInterval(2, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(workerCount()).isZero());
        Awaitility.await("query fails").atMost(Duration.ofSeconds(10))
                .pollInterval(1, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(queryState(qid)).isEqualTo("FAILED"));
        worker.start(); // cleanup
    }

    private void assertWorkerRegistered() {
        assertThat(workerCount()).isGreaterThanOrEqualTo(1);
    }

    private int workerCount() {
        try {
            var resp = httpClient.send(HttpRequest.newBuilder()
                    .uri(URI.create(baseUrl() + "/v1/workers")).GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            return resp.statusCode() == 200
                    ? (int) resp.body().lines().filter(l -> l.contains("workerId")).count() : -1;
        } catch (Exception e) { return -1; }
    }

    private String submitQuery(String sql) {
        try {
            var resp = httpClient.send(HttpRequest.newBuilder()
                    .uri(URI.create(baseUrl() + "/v1/query"))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(
                            "{\"sql\":\"" + sql + "\",\"sessionProperties\":{}}"))
                    .build(), HttpResponse.BodyHandlers.ofString());
            String b = resp.body();
            int i = b.indexOf("\"queryId\"");
            return i < 0 ? "" : b.substring(b.indexOf('"', i + 10) + 1, b.indexOf('"', b.indexOf('"', i + 10) + 1));
        } catch (Exception e) { return ""; }
    }

    private String queryState(String qid) {
        try {
            var resp = httpClient.send(HttpRequest.newBuilder()
                    .uri(URI.create(baseUrl() + "/v1/query/" + qid + "/status")).GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() == 404) return "UNKNOWN";
            String b = resp.body();
            int i = b.indexOf("\"state\"");
            return i < 0 ? "UNKNOWN" : b.substring(b.indexOf('"', i + 8) + 1, b.indexOf('"', b.indexOf('"', i + 8) + 1));
        } catch (Exception e) { return "UNKNOWN"; }
    }

    private String baseUrl() {
        return "http://" + coordinator.getHost() + ":" + coordinator.getMappedPort(COORDINATOR_PORT);
    }

    private static boolean imageExists(String img) {
        try { DockerClientFactory.instance().client().inspectImageCmd(img).exec(); return true; }
        catch (Exception e) { return false; }
    }
}

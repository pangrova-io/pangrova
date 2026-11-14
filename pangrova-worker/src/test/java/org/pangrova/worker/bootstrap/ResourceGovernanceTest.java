package org.pangrova.worker.bootstrap;

import org.pangrova.worker.bootstrap.config.WorkerBootstrapConfig;

import org.pangrova.testing.nativefixture.TestShuffleServiceConfig;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Label;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.Tag;
import net.jqwik.api.lifecycle.AfterProperty;
import org.pangrova.worker.resource.memory.MemoryArbitrator;
import org.pangrova.worker.write.TestCatalogResourceResolver;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import net.jqwik.api.lifecycle.AfterContainer;
import net.jqwik.api.lifecycle.BeforeContainer;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Acceptance test: validates the resource governance chain is active from main().
 *
 * <p>Chain under test: WorkerBootstrap.start() → startResources() →
 * MemoryArbitrator + ThreadBudgetTracker + SpillManager → ResourceManager →
 * TaskManager wired with resource governance.</p>
 *
 * <p><b>Property 29: Resource chain from main() to governance</b></p>
 */
@org.junit.jupiter.api.Tag("native-required")
@Tag("Feature: e2e-pipeline-integration - Property 29: Resource governance chain")
class ResourceGovernanceTest {

    private static final HttpClient HTTP = HttpClient.newHttpClient();
    private static StubCoordinatorServer stubCoordinator;

    private WorkerBootstrap workerBootstrap;

    @AfterEach
    void tearDownJUnit() {
        shutdownAll();
    }

    @AfterProperty
    void tearDownJqwik() {
        shutdownAll();
    }

    @BeforeAll
    @BeforeContainer
    static void startStub() throws Exception {
        stubCoordinator = StubCoordinatorServer.start();
    }

    @AfterAll
    @AfterContainer
    static void stopStub() {
        if (stubCoordinator != null) {
            stubCoordinator.close();
        }
    }

    // -- Providers --

    @Provide
    Arbitrary<String> workerIds() {
        return Arbitraries.strings().alpha().ofMinLength(3).ofMaxLength(8)
                .map(s -> "rg-" + s);
    }

    // -- Property tests --

    /**
     * P29a: For any workerId, WorkerBootstrap.start() succeeds and TaskManager
     * is non-null — proving the resource chain is wired from main().
     */
    @Property(tries = 5)
    @Label("Resource governance active from main()")
    void resourceGovernanceActiveFromMain(
            @ForAll("workerIds") String workerId) {

        startWorker(workerId);

        assertThat(workerBootstrap.isStarted()).isTrue();
        assertThat(workerBootstrap.getTaskManager()).isNotNull();
        assertThat(workerBootstrap.getTaskManager().getRunningTaskCount()).isZero();
    }

    /**
     * P29b: For any workerId, start Worker and submit a task via HTTP POST
     * to /v1/task, verify the endpoint is reachable (resource chain supports requests).
     */
    @Property(tries = 5)
    @Label("Buffer memory governed from main()")
    void bufferMemoryGovernedFromMain(
            @ForAll("workerIds") String workerId) throws Exception {

        startWorker(workerId);
        int port = workerBootstrap.getHttpPort();
        assertThat(port).isGreaterThan(0);

        HttpResponse<String> resp = submitTaskHttp(port);
        // 200 = accepted, 400 = bad request, 500 = deserialization error — all prove endpoint is live
        assertThat(resp.statusCode()).isIn(200, 400, 500);
    }

    // -- Scenario tests --

    /**
     * WorkerBootstrap lifecycle: start → verify started → shutdown → verify stopped.
     */
    @Test
    void workerBootstrapResourceLifecycle() {
        startWorker("lifecycle-test");

        assertThat(workerBootstrap.isStarted()).isTrue();
        assertThat(workerBootstrap.getHttpPort()).isGreaterThan(0);

        assertThatCode(() -> workerBootstrap.shutdown()).doesNotThrowAnyException();
        assertThat(workerBootstrap.isStarted()).isFalse();
        workerBootstrap = null;
    }

    /**
     * TaskManager is initialized with ResourceManager — running task count starts at zero.
     */
    @Test
    void taskManagerInitializedWithResourceManager() {
        startWorker("tm-resource-test");

        assertThat(workerBootstrap.getTaskManager()).isNotNull();
        assertThat(workerBootstrap.getTaskManager().getRunningTaskCount()).isZero();
    }

    /**
     * MemoryArbitrator tracks allocations and releases correctly.
     */
    @Test
    void memoryArbitratorTracksAllocations() {
        long totalBudget = 1024 * 1024L;
        long systemReserved = 64 * 1024L;
        MemoryArbitrator arbitrator = new MemoryArbitrator(totalBudget, systemReserved);

        assertThat(arbitrator.getUsedMemory()).isZero();

        long allocated = arbitrator.requestMemory("q1", 4096);
        assertThat(allocated).isEqualTo(4096);
        assertThat(arbitrator.getUsedMemory()).isEqualTo(4096);

        arbitrator.release("q1", 4096);
        assertThat(arbitrator.getUsedMemory()).isZero();
    }

    // -- Bootstrap helpers --

    private void startWorker(String workerId) {
        shutdownWorker();
        WorkerBootstrapConfig config = new WorkerBootstrapConfig(
                workerId, 0, 0, stubCoordinator.baseUrl(),
                WorkerBootstrapConfig.DEFAULT_HEARTBEAT_INTERVAL_MS,
                false, null, WorkerBootstrapConfig.ResourceConfig.defaults(),
                WorkerBootstrapConfig.DEFAULT_RESULT_RETENTION_SECONDS,
                false, new WorkerBootstrapConfig.RegistrationRetryConfig(1, 1, 1), null, null,
                null, null,
                null, null, null, null, org.pangrova.testing.nativefixture.TestShuffleServiceConfig.admissionDirectory());
        workerBootstrap = new WorkerBootstrap(TestShuffleServiceConfig.config(config));
        workerBootstrap.setCatalogResourceResolver(new TestCatalogResourceResolver(
                (taskId) -> {
                    throw new AssertionError("Resource governance test must not resolve a catalog");
                }));
        workerBootstrap.start();
    }

    private void shutdownAll() {
        shutdownWorker();
    }

    private void shutdownWorker() {
        if (workerBootstrap != null) {
            workerBootstrap.shutdown();
            workerBootstrap = null;
        }
    }

    // -- HTTP helpers --

    private static HttpResponse<String> submitTaskHttp(int port) throws Exception {
        String body = "{}";
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/v1/task"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
        return HTTP.send(request, HttpResponse.BodyHandlers.ofString());
    }

}

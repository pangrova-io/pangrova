package org.pangrova.testing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * TestContainers-based Pangrova cluster for true multi-process E2E testing.
 *
 * <p>Starts a coordinator container and N worker containers using the
 * {@code pangrova/pangrova:latest} Docker image (built from {@code docs/develop/deployment/docker/Dockerfile}).
 *
 * <p>Usage:
 * <pre>{@code
 *   try (PangrovaClusterContainer cluster = new PangrovaClusterContainer(1)) {
 *       cluster.start();
 *       String jdbcUrl = cluster.getJdbcUrl();
 *       // ... run queries ...
 *   }
 * }</pre>
 *
 * <p>For in-JVM testing (faster, no Docker image required), use {@code SqlE2ETestBase} instead.
 * This class is intended for Tasks 19/23/24 where true multi-process isolation is required.
 */
public class PangrovaClusterContainer implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(PangrovaClusterContainer.class);
    private static final DockerImageName DEFAULT_IMAGE = DockerImageName.parse("pangrova/pangrova:latest");

    private static final int COORDINATOR_HTTP_PORT = 8080;
    private static final int WORKER_HTTP_PORT = 8081;
    private static final Duration STARTUP_TIMEOUT = Duration.ofSeconds(120);

    private final DockerImageName imageName;
    private final int workerCount;
    private final Network network;

    private GenericContainer<?> coordinator;
    private final List<GenericContainer<?>> workers = new ArrayList<>();
    private boolean started;

    /**
     * Create a cluster with the default image and specified number of workers.
     *
     * @param workerCount number of worker containers (must be >= 1)
     */
    public PangrovaClusterContainer(int workerCount) {
        this(DEFAULT_IMAGE, workerCount);
    }

    /**
     * Create a cluster with a custom image and specified number of workers.
     *
     * @param imageName   Docker image to use for coordinator and workers
     * @param workerCount number of worker containers (must be >= 1)
     */
    public PangrovaClusterContainer(DockerImageName imageName, int workerCount) {
        if (workerCount < 1) {
            throw new IllegalArgumentException("workerCount must be >= 1, got: " + workerCount);
        }
        this.imageName = Objects.requireNonNull(imageName);
        this.workerCount = workerCount;
        this.network = Network.newNetwork();
    }

    /**
     * Start the cluster: coordinator first, then all workers.
     */
    public void start() {
        if (started) {
            return;
        }
        LOG.info("Starting PangrovaClusterContainer: 1 coordinator + {} workers", workerCount);

        // Coordinator
        coordinator = new GenericContainer<>(imageName)
                .withNetwork(network)
                .withNetworkAliases("coordinator")
                .withCommand("start", "coordinator")
                .withExposedPorts(COORDINATOR_HTTP_PORT)
                .withEnv("PANGROVA_JAVA_OPTS", "-Xms512m -Xmx2g")
                .withEnv("COORDINATOR_ID", "e2e-coordinator")
                .waitingFor(Wait.forHttp("/v1/health")
                        .forPort(COORDINATOR_HTTP_PORT)
                        .forStatusCode(200)
                        .withStartupTimeout(STARTUP_TIMEOUT))
                .withLogConsumer(new Slf4jLogConsumer(LOG).withPrefix("coordinator"));
        coordinator.start();

        // Workers
        for (int i = 1; i <= workerCount; i++) {
            String workerId = "e2e-worker-" + i;
            GenericContainer<?> worker = new GenericContainer<>(imageName)
                    .withNetwork(network)
                    .withNetworkAliases("worker-" + i)
                    .withCommand("start", "worker")
                    .withExposedPorts(WORKER_HTTP_PORT)
                    .withEnv("PANGROVA_JAVA_OPTS", "-Xms512m -Xmx4g -XX:MaxDirectMemorySize=2g")
                    .withEnv("WORKER_ID", workerId)
                    .withEnv("WORKER_COORDINATOR_URL", "http://coordinator:" + COORDINATOR_HTTP_PORT)
                    .dependsOn(coordinator)
                    .waitingFor(Wait.forHttp("/v1/health")
                            .forPort(WORKER_HTTP_PORT)
                            .forStatusCode(200)
                            .withStartupTimeout(STARTUP_TIMEOUT))
                    .withLogConsumer(new Slf4jLogConsumer(LOG).withPrefix(workerId));
            worker.start();
            workers.add(worker);
        }

        started = true;
        LOG.info("PangrovaClusterContainer started: coordinator={}, workers={}",
                getCoordinatorUrl(), workerCount);
    }

    /**
     * Stop all containers (workers first, then coordinator).
     */
    public void stop() {
        if (!started) {
            return;
        }
        LOG.info("Stopping PangrovaClusterContainer...");
        for (GenericContainer<?> worker : workers) {
            worker.stop();
        }
        workers.clear();
        coordinator.stop();
        network.close();
        started = false;
    }

    @Override
    public void close() {
        stop();
    }

    /**
     * Returns the coordinator HTTP URL accessible from the host.
     */
    public String getCoordinatorUrl() {
        if (!started) {
            throw new IllegalStateException("Cluster not started");
        }
        return "http://" + coordinator.getHost() + ":" + coordinator.getMappedPort(COORDINATOR_HTTP_PORT);
    }

    /**
     * Returns a JDBC connection URL for the Pangrova JDBC driver.
     */
    public String getJdbcUrl() {
        return "jdbc:pangrova://" + coordinator.getHost() + ":" + coordinator.getMappedPort(COORDINATOR_HTTP_PORT);
    }

    /**
     * Returns the coordinator container.
     */
    public GenericContainer<?> getCoordinator() {
        return coordinator;
    }

    /**
     * Returns an unmodifiable list of worker containers.
     */
    public List<GenericContainer<?>> getWorkers() {
        return Collections.unmodifiableList(workers);
    }

    /**
     * Returns the number of workers in this cluster.
     */
    public int getWorkerCount() {
        return workerCount;
    }

    /**
     * Returns whether the cluster is currently running.
     */
    public boolean isStarted() {
        return started;
    }

    /**
     * Returns the shared Docker network.
     */
    public Network getNetwork() {
        return network;
    }
}

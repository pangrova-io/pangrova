package org.pangrova.testing.testkit;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * TestContainers-based Pangrova cluster for true multi-process E2E testing.
 *
 * <p>Starts a coordinator container and N worker containers using the
 * {@code pangrova/pangrova:latest} Docker image (built from {@code deploy/docker/Dockerfile}).
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
 * Every worker has its own process, Native runtime and HTTP endpoint.
 */
public final class PangrovaClusterContainer implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(PangrovaClusterContainer.class);
    private static final ObjectMapper HEALTH_JSON = new ObjectMapper();
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
    private boolean closed;
    private boolean closing;
    private Consumer<GenericContainer<?>> coordinatorConfiguration = ignored -> {};
    private BiConsumer<Integer, GenericContainer<?>> workerConfiguration = (number, worker) -> {};

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
        if (closed || closing) throw new IllegalStateException("Cluster is closed");
        if (started) {
            return;
        }
        LOG.info("Starting PangrovaClusterContainer: 1 coordinator + {} workers", workerCount);

        // Coordinator HTTP must answer before Workers can register; full readiness
        // deliberately remains 503 until Workers and a catalog exist.
        coordinator = new RuntimeUserContainer(imageName)
                .withNetwork(network)
                .withNetworkAliases("coordinator")
                .withCommand("start", "coordinator")
                .withExposedPorts(COORDINATOR_HTTP_PORT)
                .withEnv("PANGROVA_JAVA_OPTS", "-Xms512m -Xmx2g")
                .withEnv("PANGROVA_COORDINATOR_ID", "e2e-coordinator")
                .waitingFor(Wait.forHttp("/v1/health")
                        .forPort(COORDINATOR_HTTP_PORT)
                        .forStatusCodeMatching(status -> status == 200 || status == 503)
                        .forResponsePredicate(PangrovaClusterContainer::coordinatorHttpServing)
                        .withStartupTimeout(STARTUP_TIMEOUT))
                .withLogConsumer(new Slf4jLogConsumer(LOG).withPrefix("coordinator"));
        try {
            coordinatorConfiguration.accept(coordinator);
            coordinator.start();
            startWorkers();
            started = true;
        } catch (RuntimeException | Error startupFailure) {
            try {
                close();
            } catch (RuntimeException cleanupFailure) {
                startupFailure.addSuppressed(cleanupFailure);
            }
            throw startupFailure;
        }

        LOG.info("PangrovaClusterContainer started: coordinator={}, workers={}",
                getCoordinatorUrl(), workerCount);
    }

    private void startWorkers() {
        // Workers
        for (int i = 1; i <= workerCount; i++) {
            String workerId = "e2e-worker-" + i;
            GenericContainer<?> worker = new RuntimeUserContainer(imageName)
                    .withNetwork(network)
                    .withNetworkAliases("worker-" + i)
                    .withCommand("start", "worker")
                    .withExposedPorts(WORKER_HTTP_PORT)
                    .withEnv("PANGROVA_JAVA_OPTS", "-Xms512m -Xmx4g -XX:MaxDirectMemorySize=2g")
                    .withEnv(workerEnvironment(i))
                    .dependsOn(coordinator)
                    .waitingFor(Wait.forHttp("/v1/health")
                            .forPort(WORKER_HTTP_PORT)
                            .forStatusCode(200)
                            .withStartupTimeout(STARTUP_TIMEOUT))
                    .withLogConsumer(new Slf4jLogConsumer(LOG).withPrefix(workerId));
            workers.add(worker);
            workerConfiguration.accept(i, worker);
            worker.start();
        }
    }

    /** Coordinator readiness depends on Workers and catalogs created after this process starts. */
    static boolean coordinatorHttpServing(String body) {
        if (body == null) return false;
        try {
            var status = HEALTH_JSON.readTree(body);
            return status != null && "COORDINATOR".equals(status.path("nodeType").asText())
                    && status.path("checks").path("http").isBoolean()
                    && status.path("checks").path("http").booleanValue();
        } catch (JsonProcessingException invalidHealthResponse) {
            return false;
        }
    }

    static Map<String, String> workerEnvironment(int number) {
        return Map.of("PANGROVA_WORKER_ID", "e2e-worker-" + number,
                "PANGROVA_COORDINATOR_URL", "http://coordinator:" + COORDINATOR_HTTP_PORT,
                "PANGROVA_ADVERTISED_HOST", "worker-" + number);
    }

    /** Configures real production Coordinator containers before startup. */
    public PangrovaClusterContainer configureCoordinator(Consumer<GenericContainer<?>> configuration) {
        requireConfigurable();
        coordinatorConfiguration = Objects.requireNonNull(configuration, "configuration");
        return this;
    }

    /** Configures real production Worker containers before startup; numbering starts at one. */
    public PangrovaClusterContainer configureWorkers(BiConsumer<Integer, GenericContainer<?>> configuration) {
        requireConfigurable();
        workerConfiguration = Objects.requireNonNull(configuration, "configuration");
        return this;
    }

    private void requireConfigurable() {
        if (started || coordinator != null || closed || closing) {
            throw new IllegalStateException("Cluster configuration must precede startup");
        }
    }

    /**
     * Stop all containers (workers first, then coordinator).
     */
    public void stop() {
        if (closed) {
            return;
        }
        closing = true;
        LOG.info("Stopping PangrovaClusterContainer...");
        RuntimeException failure = null;
        for (int index = workers.size() - 1; index >= 0; index--) {
            failure = closeContainer(workers.get(index), failure);
        }
        failure = closeContainer(coordinator, failure);
        if (failure != null) throw failure;
        network.close();
        workers.clear();
        started = false;
        closed = true;
    }

    private static RuntimeException closeContainer(GenericContainer<?> container, RuntimeException failure) {
        if (container == null) return failure;
        try {
            container.stop();
        } catch (RuntimeException cleanupFailure) {
            if (failure == null) return cleanupFailure;
            failure.addSuppressed(cleanupFailure);
        }
        return failure;
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
     * Returns the borrowed, live coordinator container owned by this cluster.
     * Callers must not transfer ownership or close it independently of the cluster.
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

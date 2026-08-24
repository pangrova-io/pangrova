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
package org.pangrova.testing.e2e;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.time.Duration;
import java.util.Map;

/**
 * JVM-level singleton Celeborn containers (TestContainers Singleton Container Pattern).
 *
 * <p>Manages the lifecycle of Celeborn Master + Worker containers for E2E testing.
 * Uses the same shared Network as {@link SharedTestContainers} so that the Pangrova
 * in-JVM cluster can reach Celeborn via Docker network aliases.
 *
 * <p>Container topology:
 * <pre>
 *   celeborn-master (apache/celeborn:0.6.3)  — RPC :9097, HTTP :9098
 *   celeborn-worker (apache/celeborn:0.6.3)  — dynamic ports, registers with master
 * </pre>
 *
 * <p>Startup order: Master → Worker (Worker depends on Master for registration).
 *
 * <p>Prerequisites: Docker must be available; SharedTestContainers must initialize first
 * (provides the shared Network).
 */
public final class CelebornTestContainers {

    /** Docker image for Celeborn Master and Worker. */
    private static final String CELEBORN_IMAGE = "apache/celeborn:0.6.3";

    /** Whether both Master and Worker started successfully. */
    public static final boolean AVAILABLE;

    /** Celeborn Master container. */
    public static final GenericContainer<?> MASTER;

    /** Celeborn Worker container. */
    public static final GenericContainer<?> WORKER;

    /** Master endpoint string for Celeborn client configuration (container-internal). */
    public static final String MASTER_INTERNAL_ENDPOINT = "celeborn-master:9097";

    static {
        boolean ok = false;
        GenericContainer<?> master = null;
        GenericContainer<?> worker = null;

        try {
            if (!SharedTestContainers.AVAILABLE) {
                throw new IllegalStateException(
                        "SharedTestContainers not available — cannot start Celeborn without shared network");
            }

            // --- Celeborn Master ---
            master = new GenericContainer<>(CELEBORN_IMAGE)
                    .withNetwork(SharedTestContainers.NETWORK)
                    .withNetworkAliases("celeborn-master")
                    .withExposedPorts(9097, 9098)
                    .withCommand("bash", "-c",
                            "cat > /opt/celeborn/conf/celeborn-defaults.conf << 'EOF'\n"
                            + celebornMasterConf()
                            + "\nEOF\n"
                            + "/opt/celeborn/sbin/start-master.sh && tail -F /opt/celeborn/logs/*.log")
                    .waitingFor(Wait.forHttp("/metrics/prometheus")
                            .forPort(9098)
                            .forStatusCode(200)
                            .withStartupTimeout(Duration.ofMinutes(2)))
                    .withStartupAttempts(2);

            master.start();
            System.out.println("[CelebornTestContainers] Master started: "
                    + master.getHost() + ":" + master.getMappedPort(9097));

            // --- Celeborn Worker ---
            worker = new GenericContainer<>(CELEBORN_IMAGE)
                    .withNetwork(SharedTestContainers.NETWORK)
                    .withNetworkAliases("celeborn-worker")
                    .withCommand("bash", "-c",
                            "cat > /opt/celeborn/conf/celeborn-defaults.conf << 'EOF'\n"
                            + celebornWorkerConf()
                            + "\nEOF\n"
                            + "/opt/celeborn/sbin/start-worker.sh && tail -F /opt/celeborn/logs/*.log")
                    .waitingFor(Wait.forLogMessage(
                            ".*Registered worker.*", 1)
                            .withStartupTimeout(Duration.ofMinutes(2)))
                    .dependsOn(master)
                    .withStartupAttempts(2);

            worker.start();
            System.out.println("[CelebornTestContainers] Worker started and registered with master");

            ok = true;
        } catch (Throwable e) {
            System.err.println("[CelebornTestContainers] FAILED to start Celeborn: " + e);
            e.printStackTrace(System.err);
            if (worker != null) {
                try { worker.stop(); } catch (Exception ignored) { }
                worker = null;
            }
            if (master != null) {
                try { master.stop(); } catch (Exception ignored) { }
                master = null;
            }
        }

        AVAILABLE = ok;
        MASTER = master;
        WORKER = worker;
    }

    /**
     * Returns Celeborn client connection properties for tests running in the host JVM.
     * Uses the mapped (host-accessible) port for the master RPC endpoint.
     */
    public static Map<String, String> clientProperties() {
        if (!AVAILABLE) {
            return Map.of();
        }
        return Map.of(
                "celeborn.master.endpoints",
                MASTER.getHost() + ":" + MASTER.getMappedPort(9097));
    }

    /**
     * Returns the master endpoint accessible from within the Docker network.
     * Used when Pangrova cluster components run in-JVM but connect via shared network.
     */
    public static String masterEndpointInternal() {
        return MASTER_INTERNAL_ENDPOINT;
    }

    // --- Configuration templates ---

    private static String celebornMasterConf() {
        return String.join("\n",
                "celeborn.master.host celeborn-master",
                "celeborn.master.port 9097",
                "celeborn.master.http.port 9098",
                "celeborn.worker.storage.dirs /tmp/celeborn",
                "celeborn.worker.flusher.buffer.size 256k",
                "celeborn.metrics.enabled true"
        );
    }

    private static String celebornWorkerConf() {
        return String.join("\n",
                "celeborn.master.endpoints celeborn-master:9097",
                "celeborn.worker.storage.dirs /tmp/celeborn",
                "celeborn.worker.flusher.buffer.size 256k",
                "celeborn.worker.fetch.port 0",
                "celeborn.worker.push.port 0",
                "celeborn.worker.rpc.port 0",
                "celeborn.worker.replicate.port 0",
                "celeborn.metrics.enabled true"
        );
    }

    private CelebornTestContainers() {}
}

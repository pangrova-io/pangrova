package org.pangrova.testing;

import net.jqwik.api.lifecycle.AfterContainer;
import net.jqwik.api.lifecycle.BeforeContainer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.Network;

/**
 * Abstract base for TestContainers integration tests.
 * Provides shared network and lifecycle cleanup.
 * Fails required integration tests when Docker is unavailable.
 *
 * <p>Docker discovery relies on Testcontainers built-in strategies:
 * DOCKER_HOST env var, ~/.testcontainers.properties, or docker context.
 *
 * <p>The unified JUnit/jqwik lifecycle check throws when Docker discovery fails.
 * Required integration coverage must never become a false-green skipped build.
 */
@Tag("integration")
public abstract class ContainerTestBase {

    /** Assigned by the E2E owner and borrowed by subclasses across testing modules. */
    protected static Network sharedNetwork;
    private static volatile boolean dockerReady;

    /**
     * Returns whether Docker is available after enforcing the required runtime.
     *
     * @return {@code true} when Docker is available
     * @throws IllegalStateException when Docker is unavailable
     */
    protected static boolean isDockerReady() {
        verifyDockerAvailable(dockerReady);
        return true;
    }

    static void verifyDockerAvailable(boolean available) {
        if (!available) {
            throw new IllegalStateException(
                    "Docker is required for container integration tests but is unavailable");
        }
    }

    @BeforeAll
    @BeforeContainer
    protected static void ensureDockerAvailable() {
        if (dockerReady) {
            return;
        }
        try {
            dockerReady = DockerClientFactory.instance().isDockerAvailable();
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Docker discovery failed for required container integration tests", e);
        }
        verifyDockerAvailable(dockerReady);
    }

    @AfterAll
    @AfterContainer
    static void tearDownNetwork() {
        // No-op: network is a JVM singleton managed by SqlE2ETestBase shutdown hook.
    }
}

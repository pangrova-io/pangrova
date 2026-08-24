package org.pangrova.testing;

import net.jqwik.api.lifecycle.AfterContainer;
import net.jqwik.api.lifecycle.BeforeContainer;
import net.jqwik.api.lifecycle.LifecycleContext;
import net.jqwik.api.lifecycle.SkipExecutionHook;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.Network;

/**
 * Abstract base for TestContainers integration tests.
 * Provides shared network and lifecycle cleanup.
 * Gracefully skips all tests when Docker is unavailable.
 *
 * <p>Docker discovery relies on Testcontainers built-in strategies:
 * DOCKER_HOST env var, ~/.testcontainers.properties, or docker context.
 *
 * <p>jqwik's engine executes both {@code @BeforeAll} and {@code @BeforeContainer}
 * methods, and treats any exception (including JUnit's {@code TestAbortedException}
 * from {@code assumeTrue}) as a container-level error rather than a skip.
 * Therefore this class uses a single unified method with both annotations that
 * silently sets a flag and returns early when Docker is unavailable.
 * Subclasses that need JUnit-style skipping should call
 * {@code assumeTrue(isDockerReady(), ...)} in their own {@code @BeforeAll}.
 *
 * <p>For jqwik property tests, this class implements {@link SkipExecutionHook}
 * so that properties are reported as skipped (not errored) when Docker is
 * unavailable. This replaces the previous {@code Assume.that()} approach
 * which jqwik/surefire reported as errors.
 */
@Tag("integration")
public abstract class ContainerTestBase implements SkipExecutionHook {

    protected static Network sharedNetwork;
    private static volatile boolean dockerReady;

    /**
     * Returns whether Docker is available. Subclasses should guard Docker-dependent
     * setup with this check. JUnit subclasses can use
     * {@code assumeTrue(isDockerReady())} in their {@code @BeforeAll}.
     */
    protected static boolean isDockerReady() {
        return dockerReady;
    }

    @Override
    public SkipResult shouldBeSkipped(LifecycleContext context) {
        if (!dockerReady) {
            return SkipResult.skip("Docker is not available — skipping container-based tests");
        }
        return SkipResult.doNotSkip();
    }

    @BeforeAll
    @BeforeContainer
    protected static void ensureDockerAvailable() {
        if (dockerReady) {
            return;
        }
        try {
            dockerReady = DockerClientFactory.instance().isDockerAvailable();
        } catch (Exception ignored) {
            dockerReady = false;
        }
    }

    @AfterAll
    @AfterContainer
    static void tearDownNetwork() {
        // No-op: network is a JVM singleton managed by SqlE2ETestBase shutdown hook.
    }
}

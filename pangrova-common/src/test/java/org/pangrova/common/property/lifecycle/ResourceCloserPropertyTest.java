package org.pangrova.common.property.lifecycle;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import org.pangrova.common.lifecycle.ResourceCloser;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Property tests for {@link ResourceCloser}.
 *
 * <p>Verifies two universal properties:
 * <ol>
 *   <li>{@code closeQuietly} never propagates exceptions for any {@link AutoCloseable}
 *       implementation that throws {@link Exception} subclasses</li>
 *   <li>{@code shutdownExecutor} always results in {@code executor.isShutdown() == true}</li>
 * </ol>
 *
 * <p><b>Validates: Requirements 1.1-1.7</b></p>
 */
class ResourceCloserPropertyTest {

    // ── Property 1: closeQuietly never throws for any AutoCloseable ──

    @Property(tries = 10)
    void closeQuietlyNeverThrowsForAnyCloseable(
            @ForAll("closeables") AutoCloseable resource) {
        assertThatCode(() -> ResourceCloser.closeQuietly(resource))
                .doesNotThrowAnyException();
    }

    @Property(tries = 10)
    void closeQuietlyWithLoggerNeverThrowsForAnyCloseable(
            @ForAll("closeables") AutoCloseable resource) {
        assertThatCode(() -> ResourceCloser.closeQuietly(
                resource,
                java.util.logging.Logger.getLogger("property.test"),
                "test-label"))
                .doesNotThrowAnyException();
    }

    // ── Property 2: shutdownExecutor results in isShutdown() == true ──

    @Property(tries = 10)
    void shutdownExecutorAlwaysResultsInIsShutdownTrue(
            @ForAll("executors") ExecutorService executor) {
        ResourceCloser.shutdownExecutor(executor, Duration.ofMillis(100));
        assertThat(executor.isShutdown()).isTrue();
    }

    @Property(tries = 10)
    void shutdownExecutorWithLoggerAlwaysResultsInIsShutdownTrue(
            @ForAll("executors") ExecutorService executor) {
        ResourceCloser.shutdownExecutor(
                executor,
                Duration.ofMillis(100),
                "test-executor",
                java.util.logging.Logger.getLogger("property.test.shutdown"));
        assertThat(executor.isShutdown()).isTrue();
    }

    // ── Arbitraries ──

    @Provide
    Arbitrary<AutoCloseable> closeables() {
        return Arbitraries.oneOf(
                // Normal close — no exception
                Arbitraries.just((AutoCloseable) () -> {}),
                // Throws RuntimeException
                Arbitraries.just((AutoCloseable) () -> {
                    throw new RuntimeException("runtime failure");
                }),
                // Throws checked Exception
                Arbitraries.just((AutoCloseable) () -> {
                    throw new Exception("checked failure");
                }),
                // Throws IOException
                Arbitraries.just((AutoCloseable) () -> {
                    throw new java.io.IOException("io failure");
                }),
                // Throws IllegalStateException (already closed pattern)
                Arbitraries.just((AutoCloseable) () -> {
                    throw new IllegalStateException("already closed");
                }),
                // Throws UnsupportedOperationException
                Arbitraries.just((AutoCloseable) () -> {
                    throw new UnsupportedOperationException("not supported");
                }),
                // Throws SecurityException
                Arbitraries.just((AutoCloseable) () -> {
                    throw new SecurityException("access denied");
                }),
                // Throws NullPointerException (simulating internal NPE during close)
                Arbitraries.just((AutoCloseable) () -> {
                    throw new NullPointerException("internal npe");
                })
        );
    }

    @Provide
    Arbitrary<ExecutorService> executors() {
        return Arbitraries.oneOf(
                Arbitraries.create(Executors::newSingleThreadExecutor),
                Arbitraries.create(() -> Executors.newFixedThreadPool(2)),
                Arbitraries.create(Executors::newCachedThreadPool),
                Arbitraries.create(() -> Executors.newScheduledThreadPool(1))
        );
    }
}

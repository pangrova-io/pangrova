package org.pangrova.server.coordinator.bootstrap;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import org.pangrova.server.coordinator.bootstrap.lifecycle.ExecutorLifecycleManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link ExecutorLifecycleManager}.
 * Verifies LIFO shutdown order, registration tracking, and edge cases.
 */
class ExecutorLifecycleManagerTest {

    private final ExecutorLifecycleManager manager = new ExecutorLifecycleManager(Duration.ofSeconds(2));

    @AfterEach
    void cleanup() {
        manager.close();
    }

    @Test
    void shutdownOrder_isLifo() {
        List<String> shutdownOrder = new ArrayList<>();

        ScheduledExecutorService first = trackingExecutor("LeaseRenewal", shutdownOrder);
        ScheduledExecutorService second = trackingExecutor("OrphanReclaimer", shutdownOrder);
        ScheduledExecutorService third = trackingExecutor("SpeculativeExecution", shutdownOrder);
        ScheduledExecutorService fourth = trackingExecutor("MetricReporter", shutdownOrder);

        manager.register("LeaseRenewal", first);
        manager.register("OrphanReclaimer", second);
        manager.register("SpeculativeExecution", third);
        manager.register("MetricReporter", fourth);

        manager.close();

        assertThat(shutdownOrder)
                .as("Executors should be shut down in LIFO (reverse registration) order")
                .containsExactly("MetricReporter", "SpeculativeExecution", "OrphanReclaimer", "LeaseRenewal");
    }

    @Test
    void register_returnsTheSameExecutor() {
        ScheduledExecutorService executor = createExecutor();
        ScheduledExecutorService returned = manager.register("test", executor);
        assertThat(returned).isSameAs(executor);
    }

    @Test
    void registeredCount_tracksRegistrations() {
        assertThat(manager.registeredCount()).isZero();

        manager.register("A", createExecutor());
        assertThat(manager.registeredCount()).isEqualTo(1);

        manager.register("B", createExecutor());
        assertThat(manager.registeredCount()).isEqualTo(2);
    }

    @Test
    void close_shutsDownAllExecutors() {
        ScheduledExecutorService e1 = createExecutor();
        ScheduledExecutorService e2 = createExecutor();

        manager.register("first", e1);
        manager.register("second", e2);

        manager.close();

        assertThat(e1.isShutdown()).isTrue();
        assertThat(e2.isShutdown()).isTrue();
    }

    @Test
    void close_isIdempotent() {
        ScheduledExecutorService executor = createExecutor();
        manager.register("idempotent", executor);

        manager.close();
        manager.close(); // second call should not throw

        assertThat(executor.isShutdown()).isTrue();
    }

    @Test
    void close_withNoRegistrations_doesNotThrow() {
        ExecutorLifecycleManager empty = new ExecutorLifecycleManager();
        empty.close(); // should not throw
    }

    @Test
    void register_rejectsNullName() {
        assertThatThrownBy(() -> manager.register(null, createExecutor()))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("name");
    }

    @Test
    void register_rejectsNullExecutor() {
        assertThatThrownBy(() -> manager.register("test", null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("executor");
    }

    @Test
    void constructor_rejectsNullTimeout() {
        assertThatThrownBy(() -> new ExecutorLifecycleManager(null))
                .isInstanceOf(NullPointerException.class);
    }

    private static ScheduledExecutorService createExecutor() {
        var executor = new ScheduledThreadPoolExecutor(1, r -> {
            Thread t = new Thread(r, "test-executor");
            t.setDaemon(true);
            return t;
        });
        executor.setRemoveOnCancelPolicy(true);
        return executor;
    }

    /**
     * Creates an executor that records its name into the given list when shutdownNow() is called.
     */
    private static ScheduledExecutorService trackingExecutor(String name, List<String> shutdownOrder) {
        return new ShutdownTrackingExecutor(name, shutdownOrder);
    }

    /**
     * A ScheduledThreadPoolExecutor subclass that records when shutdownNow() is called.
     */
    private static final class ShutdownTrackingExecutor extends ScheduledThreadPoolExecutor {

        private final String name;
        private final List<String> shutdownOrder;

        ShutdownTrackingExecutor(String name, List<String> shutdownOrder) {
            super(1, r -> {
                Thread t = new Thread(r, "tracking-" + name);
                t.setDaemon(true);
                return t;
            });
            setRemoveOnCancelPolicy(true);
            this.name = name;
            this.shutdownOrder = shutdownOrder;
        }

        @Override
        public List<Runnable> shutdownNow() {
            shutdownOrder.add(name);
            return super.shutdownNow();
        }
    }
}

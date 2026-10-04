package org.pangrova.coordinator.bootstrap.lifecycle;

import java.nio.channels.Pipe;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Pure process-control failures; no task execution, ACK, or database durability is simulated. */
class CoordinatorLifecycleAdmissionTest {
    @Test
    void failedDependencyRetainsCloseObligationAndSuccessfulExitIsNotRepeated() throws Exception {
        var lifecycle = new CoordinatorLifecycleAdmission();
        var pipe = Pipe.open();
        var attempts = new AtomicInteger();
        var exits = new AtomicInteger();
        lifecycle.beginStart();
        lifecycle.endStart(true);
        Runnable cleanup = () -> {
            lifecycle.step("sink", () -> {
                try { pipe.sink().close(); }
                catch (java.io.IOException failure) { throw new IllegalStateException(failure); }
                exits.incrementAndGet();
            });
            lifecycle.step("source", () -> {
                if (attempts.incrementAndGet() == 1) throw new IllegalStateException("control close boundary failure");
                try { pipe.source().close(); }
                catch (java.io.IOException failure) { throw new IllegalStateException(failure); }
            });
        };
        try {
            assertThatThrownBy(() -> lifecycle.close(cleanup)).isInstanceOf(IllegalStateException.class);
            assertThat(lifecycle.isRunning()).isFalse();
            assertThat(pipe.source().isOpen()).isTrue();
            assertThat(pipe.sink().isOpen()).isFalse();
            lifecycle.close(cleanup);
            lifecycle.close(cleanup);
            assertThat(pipe.source().isOpen()).isFalse();
            assertThat(exits).hasValue(1);
            assertThat(attempts).hasValue(2);
            assertThatThrownBy(lifecycle::beginStart).isInstanceOf(IllegalStateException.class);
        } finally { pipe.sink().close(); pipe.source().close(); }
    }

    @Test
    void dependencyCannotJoinItsOwnUnfinishedStep() {
        var lifecycle = new CoordinatorLifecycleAdmission();
        assertThatThrownBy(() -> lifecycle.step("owner", () -> lifecycle.step("owner", () -> { })))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("join itself");
        lifecycle.step("owner", () -> { });
    }

    @Test
    void invalidCallbacksAreRejectedWithoutChangingAdmission() {
        var lifecycle = new CoordinatorLifecycleAdmission();
        assertThatThrownBy(() -> lifecycle.close(null)).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> lifecycle.step("owner", null)).isInstanceOf(NullPointerException.class);
        lifecycle.beginStart();
        lifecycle.endStart(true);
        assertThat(lifecycle.isRunning()).isTrue();
        lifecycle.close(() -> { });
    }
}

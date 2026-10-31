package org.pangrova.coordinator.bootstrap.lifecycle;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Callback-control ownership tests, not database or Native execution substitutes. */
class CoordinatorCallbackLifetimeTest {
    @Test
    void timeoutRetainsTheActualEnteredCallbackUntilItsFinallyExit() throws Exception {
        var lifetime = new CoordinatorCallbackLifetime();
        var operation = lifetime.enter();
        var executor = Executors.newSingleThreadExecutor();
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var exited = new CountDownLatch(1);
        try {
            var callback = executor.submit(() -> operation.run(() -> {
                entered.countDown();
                try { assertThat(release.await(5, TimeUnit.SECONDS)).isTrue(); }
                catch (InterruptedException failure) { throw new IllegalStateException(failure); }
                finally { exited.countDown(); }
            }));
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
            assertThatThrownBy(() -> lifetime.close(1)).isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("not actually exited");
            assertThatThrownBy(operation::close).isInstanceOf(IllegalStateException.class);
            assertThatThrownBy(lifetime::enter).isInstanceOf(IllegalStateException.class);
            release.countDown();
            callback.get(5, TimeUnit.SECONDS);
            lifetime.close(5000);
            assertThat(exited.getCount()).isZero();
        } finally {
            release.countDown();
            executor.shutdown();
            assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
            lifetime.close(5000);
        }
    }

    @Test
    void acceptedButNotTransferredCallbackMustBeReturnedBeforeDrain() {
        var lifetime = new CoordinatorCallbackLifetime();
        var operation = lifetime.enter();
        assertThatThrownBy(() -> lifetime.close(1)).isInstanceOf(IllegalStateException.class);
        operation.close();
        lifetime.close(5000);
        operation.close();
        assertThatThrownBy(() -> operation.run(() -> { throw new AssertionError("returned callback ran"); }))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void callbackCannotJoinItselfButActualFinallyExitAllowsExternalClose() {
        var lifetime = new CoordinatorCallbackLifetime();
        var operation = lifetime.enter();
        operation.run(() -> assertThatThrownBy(() -> lifetime.close(5000))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("own exit"));
        lifetime.close(5000);
    }
}

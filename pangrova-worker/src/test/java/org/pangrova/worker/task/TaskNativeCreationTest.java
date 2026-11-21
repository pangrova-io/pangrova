package org.pangrova.worker.task;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Actual Runnable admission/exit barriers; not a Native execution fixture. */
class TaskNativeCreationTest {
    @Test
    void preEntrySealPreventsLateRunnableFromEnteringCreation() {
        var creation = new TaskNativeCreation();
        var publishedHandle = new AtomicLong();
        assertThat(creation.sealWithoutOwnerOrAwaitExit()).isTrue();
        assertThatThrownBy(creation::begin).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> creation.publish(17, publishedHandle::set)).isInstanceOf(IllegalStateException.class);
        assertThat(publishedHandle).hasValue(0);
    }

    @Test
    void enteredCreationMustReallyExitBeforeNoOwnerIsAccepted() throws Exception {
        var creation = new TaskNativeCreation();
        creation.begin();
        var entered = new CountDownLatch(1);
        var exit = new CountDownLatch(1);
        var drained = new AtomicBoolean();
        var rollbackPublished = new AtomicBoolean();
        var executor = Executors.newSingleThreadExecutor(work -> new Thread(work, "native-creation-exit-probe"));
        try {
            var waiter = executor.submit(() -> {
                entered.countDown();
                drained.set(creation.sealWithoutOwnerOrAwaitExit());
                exit.countDown();
            });
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(exit.getCount()).isEqualTo(1);
            creation.failedBeforeNativeEntry();
            rollbackPublished.set(true);
            waiter.get(5, TimeUnit.SECONDS);
            assertThat(drained).isTrue();
            assertThat(exit.getCount()).isZero();
            assertThatThrownBy(creation::begin).isInstanceOf(IllegalStateException.class);
        } finally {
            if (!rollbackPublished.get()) creation.failedBeforeNativeEntry();
            executor.shutdownNow();
            assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void unknownEnteredFailureDoesNotBecomeNoOwnerEvidence() {
        var creation = new TaskNativeCreation();
        creation.begin();
        var failure = new IllegalStateException("actual creation call failed before returning evidence");
        creation.failed(failure);
        assertThatThrownBy(creation::sealWithoutOwnerOrAwaitExit)
                .isInstanceOf(java.util.concurrent.CompletionException.class).hasCause(failure);
        assertThatThrownBy(creation::begin).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void exactRecoveryCanResolveEnteredFailureWithAnActualPositiveOwner() {
        var creation = new TaskNativeCreation();
        var publishedHandle = new AtomicLong();
        creation.begin();
        creation.failed(new IllegalStateException("response transfer failed"));
        creation.resolveEnteredFailure(23, publishedHandle::set);
        assertThat(publishedHandle).hasValue(23);
        assertThat(creation.sealWithoutOwnerOrAwaitExit()).isFalse();
        assertThatThrownBy(() -> creation.resolveEnteredFailure(24, publishedHandle::set))
                .isInstanceOf(IllegalStateException.class);
        assertThat(publishedHandle).hasValue(23);
    }

    @Test
    void exactRecoveryNoOwnerFactIsDistinctFromAnUnresolvedException() {
        var creation = new TaskNativeCreation();
        var publishedHandle = new AtomicLong(19);
        creation.begin();
        creation.failed(new IllegalStateException("entered JNI without a response"));
        creation.resolveEnteredFailure(0, publishedHandle::set);
        assertThat(publishedHandle).hasValue(0);
        assertThat(creation.sealWithoutOwnerOrAwaitExit()).isTrue();
        assertThatThrownBy(creation::begin).isInstanceOf(IllegalStateException.class);
    }
}

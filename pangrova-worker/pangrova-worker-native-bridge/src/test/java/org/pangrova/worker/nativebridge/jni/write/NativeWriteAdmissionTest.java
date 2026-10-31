package org.pangrova.worker.nativebridge.jni.write;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Exercises the production ownership state machine, without connector substitutes or JNI. */
class NativeWriteAdmissionTest {
    @Test
    void invalidCanonicalTaskCannotClaimAdmissionOrTriggerTaskCleanup() {
        var admission = new NativeWriteAdmission();
        for (String taskId : new String[]{null, "", "  "}) {
            assertThatThrownBy(() -> admission.enter(taskId))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Canonical taskId");
            assertThatThrownBy(() -> admission.beginRelease(taskId))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Canonical taskId");
        }
        admission.beginCleanup(false);
        admission.endCleanup();
    }

    @Test
    void nestedClaimsCannotReleaseTheTaskUntilEveryOwnerLeaves() {
        var admission = new NativeWriteAdmission();
        admission.enter("task");
        admission.enter("task");
        admission.leave("task");
        assertBusy(() -> admission.beginRelease("task"));
        assertBusy(() -> admission.beginCleanup(false));
        admission.leave("task");
        admission.beginRelease("task");
        admission.endRelease("task");
        admission.beginCleanup(false);
        admission.endCleanup();
        assertThatThrownBy(() -> admission.leave("task"))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("no active owner");
    }

    @Test
    void taskReleaseBlocksOnlyItsOwnAdmissionButAlsoBlocksGlobalCleanup() {
        var admission = new NativeWriteAdmission();
        admission.beginRelease("first");
        assertBusy(() -> admission.beginRelease("first"));
        assertBusy(() -> admission.enter("first"));
        admission.enter("second");
        admission.leave("second");
        assertBusy(() -> admission.beginCleanup(false));
        admission.endRelease("first");
        admission.enter("first");
        admission.leave("first");
    }

    @Test
    void temporaryCleanupRejectsAdmissionAndReleaseThenReopensAfterActualExit() {
        var admission = new NativeWriteAdmission();
        admission.beginCleanup(false);
        assertBusy(() -> admission.enter("task"));
        assertBusy(() -> admission.beginRelease("task"));
        assertBusy(() -> admission.beginCleanup(false));
        admission.endCleanup();
        assertThatCode(() -> admission.enter("task")).doesNotThrowAnyException();
        admission.leave("task");
    }

    @Test
    void failedCloseSealsAdmissionWithoutReleasingTheInFlightOwner() {
        var admission = new NativeWriteAdmission();
        admission.enter("task");
        assertBusy(() -> admission.beginCleanup(true));
        assertClosed(admission);
        assertBusy(() -> admission.beginRelease("task"));
        assertBusy(() -> admission.beginCleanup(true));
        admission.leave("task");
        admission.beginCleanup(true);
        admission.endCleanup();
        assertClosed(admission);
        admission.beginCleanup(true);
        admission.endCleanup();
    }

    @Test
    void failedCloseDuringTaskReleasePreservesItsClaimUntilReleaseExits() {
        var admission = new NativeWriteAdmission();
        admission.beginRelease("task");
        assertBusy(() -> admission.beginCleanup(true));
        assertClosed(admission);
        assertBusy(() -> admission.beginRelease("task"));
        admission.endRelease("task");
        admission.beginCleanup(true);
        admission.endCleanup();
    }

    @Test
    void concurrentTaskClaimsRemainOwnedAfterNonBlockingCloseRefusal() throws Exception {
        var admission = new NativeWriteAdmission();
        var executor = Executors.newFixedThreadPool(2);
        var entered = new CountDownLatch(2);
        var release = new CountDownLatch(1);
        try {
            var first = executor.submit(() -> holdClaim(admission, "first", entered, release));
            var second = executor.submit(() -> holdClaim(admission, "second", entered, release));
            assertThat(entered.await(10, TimeUnit.SECONDS)).isTrue();
            assertBusy(() -> admission.beginCleanup(true));
            assertBusy(() -> admission.beginRelease("first"));
            assertBusy(() -> admission.beginRelease("second"));
            assertClosed(admission);
            release.countDown();
            first.get(10, TimeUnit.SECONDS);
            second.get(10, TimeUnit.SECONDS);
            admission.beginCleanup(true);
            admission.endCleanup();
        } finally {
            release.countDown();
            executor.shutdown();
            assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }
    }

    private static Void holdClaim(NativeWriteAdmission admission, String taskId,
            CountDownLatch entered, CountDownLatch release) throws InterruptedException {
        admission.enter(taskId);
        try {
            entered.countDown();
            assertThat(release.await(10, TimeUnit.SECONDS)).isTrue();
        } finally {
            admission.leave(taskId);
        }
        return null;
    }

    private static void assertBusy(org.assertj.core.api.ThrowableAssert.ThrowingCallable action) {
        assertThatThrownBy(action).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("in progress");
    }

    private static void assertClosed(NativeWriteAdmission admission) {
        assertThatThrownBy(admission::requireOpen).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("closed");
        assertThatThrownBy(() -> admission.enter("new-task")).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("closed");
    }
}

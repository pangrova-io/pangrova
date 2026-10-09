package org.pangrova.worker.execution;

import org.pangrova.testing.nativefixture.ActualNativeTaskFixture;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real JNI cleanup must remain idempotent under concurrent Worker shutdown. */
@Tag("native-required")
@ResourceLock(ActualNativeTaskFixture.RUNTIME_LOCK)
class NativeTaskExecutorConcurrentCloseTest {

    @Test
    void competingFixturesCannotInitializeOrRetireTheSameRuntime() throws Exception {
        // Load the actual process library at the bootstrap boundary. The race
        // below concerns two runtime owners, not first-use DLL extraction.
        assertThat(org.pangrova.worker.jni.NativeRuntime.isInitialized()).isFalse();
        var ready = new CountDownLatch(2);
        var go = new CountDownLatch(1);
        var owners = new java.util.concurrent.ConcurrentLinkedQueue<ActualNativeTaskFixture>();
        var rejected = new java.util.concurrent.ConcurrentLinkedQueue<Throwable>();
        var creators = new java.util.ArrayList<Thread>();
        for (int index = 0; index < 2; index++) {
            var creator = new Thread(() -> {
                ready.countDown();
                try {
                    if (!go.await(5, TimeUnit.SECONDS)) throw new IllegalStateException("Fixture start timed out");
                    owners.add(new ActualNativeTaskFixture());
                } catch (Throwable failure) {
                    rejected.add(failure);
                }
            }, "competing-native-fixture-" + index);
            creators.add(creator);
            creator.start();
        }
        try {
            assertThat(ready.await(5, TimeUnit.SECONDS)).isTrue();
            go.countDown();
            for (var creator : creators) {
                creator.join(5000);
                assertThat(creator.isAlive()).isFalse();
            }
            assertThat(owners).hasSize(1);
            assertThat(rejected).singleElement().satisfies(failure -> assertThat(failure)
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("different fixture still owns"));
            var owner = owners.element();
            assertThat(owner.createPending(owner.valuesRequest(false))).isPositive();
        } finally {
            go.countDown();
            for (var creator : creators) creator.join(5000);
            for (var owner : owners) owner.close();
        }
        try (var next = new ActualNativeTaskFixture()) {
            assertThat(next.createPending(next.valuesRequest(false))).isPositive();
        }
    }

    @Test
    void concurrentCloseCallsAreIdempotent() throws Exception {
        try (var fixture = new ActualNativeTaskFixture()) {
            long handle = fixture.createPending(fixture.valuesRequest(false));
            fixture.stopTransports();
            var ready = new CountDownLatch(10);
            var go = new CountDownLatch(1);
            var done = new CountDownLatch(10);
            var failure = new AtomicReference<Throwable>();
            var closers = new java.util.ArrayList<Thread>();
            for (int index = 0; index < 10; index++) {
                var closer = new Thread(() -> {
                    ready.countDown();
                    try {
                        if (!go.await(5, TimeUnit.SECONDS)) {
                            throw new IllegalStateException("Concurrent close start timed out");
                        }
                        fixture.executor().close();
                    } catch (Throwable error) {
                        failure.compareAndSet(null, error);
                    } finally {
                        done.countDown();
                    }
                }, "close-thread-" + index);
                closers.add(closer);
                closer.start();
            }
            try {
                assertThat(ready.await(5, TimeUnit.SECONDS)).isTrue();
                go.countDown();
                assertThat(done.await(5, TimeUnit.SECONDS)).isTrue();
            } finally {
                go.countDown();
                for (var closer : closers) {
                    closer.join(5000);
                    assertThat(closer.isAlive()).isFalse();
                }
            }
            assertThat(failure.get()).isNull();
            NativeTaskExecutorReleaseRaceTest.assertNativeHandleGone(handle);
        }
    }

    @Test
    void createTaskAfterCloseReportsVerifiedAbsenceOfNativeOwner() {
        try (var fixture = new ActualNativeTaskFixture()) {
            var request = fixture.valuesRequest(false);
            fixture.stopTransports();
            fixture.executor().close();
            assertThatThrownBy(() -> fixture.executor().createTask(request.toByteArray()))
                    .isInstanceOfSatisfying(NativeTaskCreationFailure.class, failure -> {
                        assertThat(failure.actualHandle()).isZero();
                        assertThat(failure.withoutNativeOwner()).isTrue();
                        assertThat(failure.getCause()).isInstanceOf(IllegalStateException.class)
                                .hasMessageContaining("closed");
                    });
        }
    }
}

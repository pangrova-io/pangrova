package org.pangrova.shuffle.server;

import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.pangrova.protocol.v2.ExecutionOwnerIdentity;
import org.pangrova.protocol.v2.shuffle.ReadBatchResponse;
import org.pangrova.shuffle.config.ShuffleServiceConfig;

import java.lang.reflect.Proxy;
import java.time.Clock;
import java.util.UUID;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class ShuffleReadPumpTest {
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void readClosurePreservesTerminalOutcomeWhenDurableReleaseFails(boolean cancelled) {
        var releaseCalls = new AtomicInteger();
        var store = (ShuffleLifecycleStore) Proxy.newProxyInstance(
                ShuffleLifecycleStore.class.getClassLoader(), new Class<?>[]{ShuffleLifecycleStore.class},
                (proxy, method, args) -> {
                    if ("releaseRead".equals(method.getName())) {
                        releaseCalls.incrementAndGet();
                        throw new IllegalStateException("database unavailable during release");
                    }
                    throw new UnsupportedOperationException(method.getName());
                });
        var clock = Clock.systemUTC();
        var lease = new ShuffleReadLease(store, "query", ExecutionOwnerIdentity.getDefaultInstance(),
                new ShuffleLifecycleStore.ReadPin(UUID.randomUUID(), 1000),
                clock, System.nanoTime(), clock.millis());
        // One manifest entry makes the memory reservation nonzero.
        var partition = new ShuffleServiceModel.FinalizedPartition(0, 16, 1, 0, new byte[32]);
        var publication = new ShuffleServiceModel.PartitionPublication(
                partition, new byte[ShufflePartitionManifest.rootSize(1)]);
        var published = new ShuffleServiceModel.PublishedPartition(1, 1, new byte[32], publication);
        var result = new ShuffleServiceEngine.ReadResult("shuffle", published, 0,
                clock.millis() + 1000, clock.millis() + 1000, lease);
        var response = new CapturingResponse();
        var indexMemory = new Semaphore(10_000);
        var pump = new ShuffleReadPump(null, result, response, ignored -> {
            throw new RejectedExecutionException("queue full");
        }, new Semaphore(3), indexMemory, new ShuffleServiceConfig.Limits(1, 1, 3, 1, 1, 1));

        assertThat(indexMemory.availablePermits()).isLessThan(10_000);
        if (cancelled) {
            pump.cancel();
            assertThat(response.failure).isNull();
        } else {
            pump.schedule();
            assertThat(Status.fromThrowable(response.failure).getCode()).isEqualTo(Status.Code.RESOURCE_EXHAUSTED);
            assertThat(response.failure.getSuppressed()).singleElement().isInstanceOf(IllegalStateException.class);
        }
        assertThat(releaseCalls.get()).isEqualTo(1);
        assertThat(indexMemory.availablePermits()).isEqualTo(10_000);
        pump.schedule();
        assertThat(releaseCalls.get()).isEqualTo(1);
    }

    private static final class CapturingResponse extends ServerCallStreamObserver<ReadBatchResponse> {
        private Throwable failure;
        @Override public boolean isReady() { return true; }
        @Override public boolean isCancelled() { return false; }
        @Override public void setOnCancelHandler(Runnable action) {}
        @Override public void setOnReadyHandler(Runnable action) {}
        @Override public void setCompression(String compression) {}
        @Override public void disableAutoInboundFlowControl() {}
        @Override public void request(int count) {}
        @Override public void setMessageCompression(boolean enabled) {}
        @Override public void onNext(ReadBatchResponse value) {}
        @Override public void onError(Throwable error) { failure = error; }
        @Override public void onCompleted() {}
    }
}

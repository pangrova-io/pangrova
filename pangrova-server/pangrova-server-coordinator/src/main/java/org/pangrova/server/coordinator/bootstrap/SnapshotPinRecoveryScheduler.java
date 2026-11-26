package org.pangrova.server.coordinator.bootstrap;

import org.pangrova.common.concurrent.ManagedExecutorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

/** Owns a single maintenance term's snapshot recovery cycle. */
final class SnapshotPinRecoveryScheduler implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(SnapshotPinRecoveryScheduler.class);

    private final ScheduledExecutorService executor;
    private final BooleanSupplier holdsEpoch;
    private final Consumer<BooleanSupplier> recover;

    SnapshotPinRecoveryScheduler(BooleanSupplier holdsEpoch, Consumer<BooleanSupplier> recover) {
        this(ManagedExecutorFactory.newSingleThreadScheduledExecutor("pangrova-snapshot-pin-recovery"),
                holdsEpoch, recover);
    }

    SnapshotPinRecoveryScheduler(ScheduledExecutorService executor, BooleanSupplier holdsEpoch,
                                 Consumer<BooleanSupplier> recover) {
        this.executor = executor;
        this.holdsEpoch = holdsEpoch;
        this.recover = recover;
    }

    void start() {
        executor.scheduleWithFixedDelay(this::runSafely, 0, 5, TimeUnit.MINUTES);
    }

    private boolean stillLeader() {
        return !executor.isShutdown() && holdsEpoch.getAsBoolean();
    }

    private void runSafely() {
        try {
            if (stillLeader()) {
                recover.accept(this::stillLeader);
            }
        } catch (RuntimeException failure) {
            LOG.error("event=index_snapshot_pin_recovery_failed error={}", failure.getMessage(), failure);
        }
    }

    @Override
    public void close() {
        // Stop new work; do not interrupt an already claimed connector operation.
        executor.shutdown();
    }
}

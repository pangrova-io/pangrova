package org.pangrova.shuffle.pangrova.client;

import org.pangrova.shuffle.pangrova.client.exception.RemoteShuffleClientException;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import org.pangrova.common.concurrency.GuardedBy;
import org.pangrova.common.concurrency.ThreadSafe;

/** Pool-owned actual channel; borrower counts are guarded by the pool admission monitor. */
@ThreadSafe
final class RemoteShuffleClientOwner {
    volatile PangrovaShuffleClient client;
    final RemoteShuffleChannelAdmission.Reservation reservation;
    @GuardedBy("RemoteShuffleClientPool.admission")
    int borrowers;
    @GuardedBy("RemoteShuffleClientPool.admission")
    boolean retired;
    private final AtomicReference<CompletableFuture<Void>> drain = new AtomicReference<>();

    RemoteShuffleClientOwner(RemoteShuffleChannelAdmission.Reservation reservation) {
        this.reservation = reservation;
    }

    void closeChannel() {
        var attempt = new CompletableFuture<Void>();
        if (drain.compareAndSet(null, attempt)) {
            try {
                var channel = reservation.channel();
                if (channel != null) PangrovaShuffleClient.closeChannel(channel);
                reservation.releaseTerminated();
                attempt.complete(null);
            } catch (RuntimeException | Error failure) {
                attempt.completeExceptionally(failure);
                throw failure;
            } finally {
                drain.compareAndSet(attempt, null);
            }
        } else {
            awaitConcurrentDrain();
        }
        if (!isTerminated()) {
            throw new RemoteShuffleClientException("Remote shuffle channel has not terminated");
        }
        reservation.releaseTerminated();
    }

    private void awaitConcurrentDrain() {
        var active = drain.get();
        if (active == null) {
            // A completed concurrent drain may have just cleared its handoff.
            if (!isTerminated()) closeChannel();
            return;
        }
        try {
            active.get(10, TimeUnit.SECONDS);
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new RemoteShuffleClientException("Concurrent channel drain interrupted", failure);
        } catch (ExecutionException failure) {
            throw new RemoteShuffleClientException("Concurrent channel drain failed", failure.getCause());
        } catch (TimeoutException failure) {
            throw new RemoteShuffleClientException("Concurrent channel drain has not exited", failure);
        }
    }

    private boolean isTerminated() {
        var channel = reservation.channel();
        return channel == null || channel.isTerminated();
    }
}

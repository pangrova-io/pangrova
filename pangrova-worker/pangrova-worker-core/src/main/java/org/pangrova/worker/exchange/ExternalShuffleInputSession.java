package org.pangrova.worker.exchange;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.pangrova.shuffle.pangrova.client.RemoteShuffleExchangeSource;

/** Retains all readers until RPC terminal callbacks and actual pump exits. */
final class ExternalShuffleInputSession implements AutoCloseable {

    private final Set<RemoteShuffleExchangeSource> sources = ConcurrentHashMap.newKeySet();
    private final Set<ExternalShuffleInputPump> pumps = ConcurrentHashMap.newKeySet();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final CountDownLatch bindingExited = new CountDownLatch(1);

    void addSource(RemoteShuffleExchangeSource source) {
        sources.add(source);
        if (closed.get()) {
            source.revoke();
            throw new IllegalStateException("Exchange input session is closed");
        }
    }

    void addPump(ExternalShuffleInputPump pump) {
        pumps.add(pump);
        if (closed.get()) pump.cancel();
    }

    void bindingFinished() {
        bindingExited.countDown();
    }

    boolean isClosed() {
        return closed.get();
    }

    @Override
    public void close() {
        closed.set(true);
        RuntimeException failure = revokeSources();
        pumps.forEach(ExternalShuffleInputPump::cancel);
        try {
            awaitBindingAndPumps();
        } catch (RuntimeException joinFailure) {
            // Revocation stops admission, but a running binding/pump may still
            // access its source. Keep every reader and channel lease for retry.
            throw combine(failure, joinFailure);
        }
        failure = combine(failure, closeSources());
        if (failure != null) throw failure;
        sources.clear();
        pumps.clear();
    }

    private void awaitBindingAndPumps() {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        try {
            if (!bindingExited.await(5, TimeUnit.SECONDS)) {
                throw new IllegalStateException("External exchange binding has not exited");
            }
            // Binding may have registered an owner while admission was revoked.
            RuntimeException revokeFailure = revokeSources();
            pumps.forEach(ExternalShuffleInputPump::cancel);
            RuntimeException failure = revokeFailure;
            for (ExternalShuffleInputPump pump : pumps) {
                try {
                    pump.awaitExit(deadline);
                } catch (RuntimeException joinFailure) {
                    if (failure == null) failure = joinFailure;
                    else failure.addSuppressed(joinFailure);
                }
            }
            if (failure != null) throw failure;
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while draining exchange readers", failure);
        }
    }

    private RuntimeException closeSources() {
        RuntimeException failure = null;
        for (RemoteShuffleExchangeSource source : sources) {
            try {
                source.close();
                sources.remove(source);
            } catch (RuntimeException closeFailure) {
                if (failure == null) failure = closeFailure;
                else failure.addSuppressed(closeFailure);
            }
        }
        return failure;
    }

    private RuntimeException revokeSources() {
        RuntimeException failure = null;
        for (RemoteShuffleExchangeSource source : sources) {
            try {
                source.revoke();
            } catch (RuntimeException revokeFailure) {
                failure = combine(failure, revokeFailure);
            }
        }
        return failure;
    }

    private static RuntimeException combine(RuntimeException first, RuntimeException next) {
        if (next == null) return first;
        if (first == null) return next;
        if (first == next) return first;
        first.addSuppressed(next);
        return first;
    }
}

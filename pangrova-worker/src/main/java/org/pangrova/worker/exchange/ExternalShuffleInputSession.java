package org.pangrova.worker.exchange;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.pangrova.worker.exchange.remote.RemoteShuffleExchangeSource;

/** Owns cancellable external-shuffle readers for one Native task handle. */
final class ExternalShuffleInputSession implements AutoCloseable {

    private final AtomicInteger remaining;
    private final Set<RemoteShuffleExchangeSource> sources = ConcurrentHashMap.newKeySet();
    private final Set<Future<?>> futures = ConcurrentHashMap.newKeySet();
    private final AtomicBoolean closed = new AtomicBoolean();

    ExternalShuffleInputSession(int sourceCount) {
        this.remaining = new AtomicInteger(sourceCount);
    }

    void addSource(RemoteShuffleExchangeSource source) {
        if (closed.get()) {
            source.close();
            throw new IllegalStateException("Exchange input session is closed");
        }
        sources.add(source);
    }

    void addFuture(Future<?> future) {
        if (closed.get()) {
            future.cancel(true);
        } else {
            futures.add(future);
        }
    }

    void sourceFinished(RemoteShuffleExchangeSource source) {
        sources.remove(source);
        remaining.decrementAndGet();
    }

    boolean isComplete() {
        return remaining.get() == 0;
    }

    boolean isClosed() {
        return closed.get();
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        sources.forEach(RemoteShuffleExchangeSource::close);
        futures.forEach(future -> future.cancel(true));
        sources.clear();
        futures.clear();
    }
}

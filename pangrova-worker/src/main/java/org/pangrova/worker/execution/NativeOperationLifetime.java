package org.pangrova.worker.execution;

import org.pangrova.common.concurrency.GuardedBy;
import org.pangrova.common.concurrency.ThreadSafe;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/** Short-lock admission and actual exit barriers for one Native resource owner. */
@ThreadSafe
final class NativeOperationLifetime {
    @GuardedBy("this")
    private final Map<Thread, Integer> readers = new HashMap<>();

    @GuardedBy("this")
    private final Set<Thread> waiters = new HashSet<>();

    @GuardedBy("this")
    private int entered;

    @GuardedBy("this")
    private boolean sealed;

    @GuardedBy("this")
    private CompletableFuture<Void> readerExit = CompletableFuture.completedFuture(null);

    @GuardedBy("this")
    private Exclusive exclusive;

    Lease enter() {
        var lease = new Lease(this, Thread.currentThread());
        var nextExit = new CompletableFuture<Void>();
        synchronized (this) {
            if (sealed || exclusive != null) {
                throw new IllegalStateException("Native owner operation admission is sealed");
            }
            readers.put(lease.thread, Math.addExact(readers.getOrDefault(lease.thread, 0), 1));
            if (entered == 0) {
                readerExit = nextExit;
            }
            entered++;
        }
        return lease;
    }

    /** Cleanup remains leased after failed retirement; execution/read admission stays sealed. */
    Lease tryEnterCleanup() {
        var lease = new Lease(this, Thread.currentThread());
        var nextExit = new CompletableFuture<Void>();
        synchronized (this) {
            if (exclusive != null) {
                requireNotOwnOperation(lease.thread);
                return null;
            }
            readers.put(lease.thread, Math.addExact(readers.getOrDefault(lease.thread, 0), 1));
            if (entered == 0) readerExit = nextExit;
            entered++;
        }
        return lease;
    }

    void awaitCleanupExit() {
        CompletableFuture<Void> exit;
        synchronized (this) {
            requireNotOwnOperation(Thread.currentThread());
            exit = exclusive == null ? CompletableFuture.completedFuture(null) : exclusive.exit;
        }
        awaitExit(exit);
    }

    Exclusive claim(boolean seal) {
        var candidate = new Exclusive(this, Thread.currentThread());
        while (true) {
            CompletableFuture<Void> existingExit;
            synchronized (this) {
                requireNotOwnOperation(candidate.thread);
                if (exclusive == null) {
                    candidate.readersExited = readerExit;
                    exclusive = candidate;
                    if (seal) {
                        sealed = true;
                    }
                    return candidate;
                }
                existingExit = exclusive.exit;
            }
            awaitExit(existingExit);
        }
    }

    void sealAndAwait() {
        CompletableFuture<Void> operationsExited;
        CompletableFuture<Void> readersExited;
        synchronized (this) {
            requireNotOwnOperation(Thread.currentThread());
            sealed = true;
            operationsExited =
                    exclusive == null ? CompletableFuture.completedFuture(null) : exclusive.exit;
            readersExited = readerExit;
        }
        awaitExit(operationsExited);
        awaitExit(readersExited);
    }

    private void requireNotOwnOperation(Thread thread) {
        if (readers.containsKey(thread) || (exclusive != null && exclusive.thread == thread)) {
            throw new IllegalStateException(
                    "Cannot join a Native operation from its own entered thread");
        }
    }

    private void awaitExit(CompletableFuture<Void> exit) {
        if (exit.isDone()) {
            exit.join();
            return;
        }
        var thread = Thread.currentThread();
        synchronized (this) {
            waiters.add(thread);
        }
        try {
            exit.join();
        } finally {
            synchronized (this) {
                waiters.remove(thread);
            }
        }
    }

    private synchronized boolean blocks(Thread thread) {
        return waiters.contains(thread);
    }

    private void leave(Lease lease) {
        CompletableFuture<Void> completed = null;
        synchronized (this) {
            if (lease.closed) {
                return;
            }
            int count = readers.get(lease.thread);
            if (count == 1) {
                readers.remove(lease.thread);
            } else {
                readers.put(lease.thread, count - 1);
            }
            lease.closed = true;
            if (--entered == 0) {
                completed = readerExit;
            }
        }
        if (completed != null) {
            completed.complete(null);
        }
    }

    private void finish(Exclusive claim) {
        synchronized (this) {
            if (claim.closed) {
                return;
            }
            if (exclusive != claim) {
                throw new IllegalStateException("Native operation claim lost its owner");
            }
            exclusive = null;
            claim.closed = true;
        }
        claim.exit.complete(null);
    }

    static final class Lease implements AutoCloseable {
        private final NativeOperationLifetime lifetime;
        private final Thread thread;

        @GuardedBy("lifetime")
        private boolean closed;

        private Lease(NativeOperationLifetime lifetime, Thread thread) {
            this.lifetime = lifetime;
            this.thread = thread;
        }

        boolean blocks(Thread waitingThread) {
            return lifetime.blocks(waitingThread);
        }

        @Override
        public void close() {
            lifetime.leave(this);
        }
    }

    static final class Exclusive implements AutoCloseable {
        private final NativeOperationLifetime lifetime;
        private final Thread thread;
        private final CompletableFuture<Void> exit = new CompletableFuture<>();
        private CompletableFuture<Void> readersExited;

        @GuardedBy("lifetime")
        private boolean closed;

        private Exclusive(NativeOperationLifetime lifetime, Thread thread) {
            this.lifetime = lifetime;
            this.thread = thread;
        }

        void awaitReaders() {
            lifetime.awaitExit(readersExited);
        }

        @Override
        public void close() {
            lifetime.finish(this);
        }
    }
}

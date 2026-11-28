package org.pangrova.common.concurrency;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.TreeSet;
import java.util.concurrent.locks.ReentrantLock;

import org.pangrova.common.api.annotation.Internal;

/**
 * Ordered, bounded process mutexes for resource identities shared across provider classloaders.
 * Participants must delegate Common to the same parent loader. Acquire all identities together,
 * before opening resource handles; a lease is non-reentrant and belongs to its acquiring thread.
 */
@Internal
public final class ProcessResourceLocks {
    private static final int STRIPES = 256;
    private static final List<ReentrantLock> LOCKS = java.util.stream.IntStream.range(0, STRIPES)
            .mapToObj(ignored -> new ReentrantLock()).toList();

    private ProcessResourceLocks() { }

    /** Admits resource handles only after all process mutexes have been acquired in a total order. */
    public static Lease acquire(Collection<String> identities) throws InterruptedException {
        Objects.requireNonNull(identities, "identities");
        var indices = new TreeSet<Integer>();
        for (String identity : identities) {
            indices.add(Math.floorMod(Objects.requireNonNull(identity, "identity").hashCode(), STRIPES));
        }
        var locks = indices.stream().map(LOCKS::get).toList();
        int acquired = 0;
        try {
            for (var lock : locks) {
                if (lock.isHeldByCurrentThread()) {
                    throw new IllegalStateException("Process resource leases are not reentrant");
                }
                lock.lockInterruptibly();
                acquired++;
            }
            return new Lease(Thread.currentThread(), locks);
        } catch (InterruptedException | RuntimeException | Error failure) {
            release(locks, acquired);
            throw failure;
        }
    }

    private static void release(List<ReentrantLock> acquired, int count) {
        for (int index = count - 1; index >= 0; index--) acquired.get(index).unlock();
    }

    /** Releases only this thread's successful admission; repeated owner closure is harmless. */
    @Internal
    public static final class Lease implements AutoCloseable {
        private final Thread owner;
        private final List<ReentrantLock> acquired;
        private boolean closed;

        private Lease(Thread owner, List<ReentrantLock> acquired) {
            this.owner = owner;
            this.acquired = acquired;
        }

        @Override
        public void close() {
            if (Thread.currentThread() != owner) {
                throw new IllegalStateException("Resource lease belongs to another thread");
            }
            if (closed) return;
            closed = true;
            release(acquired, acquired.size());
        }
    }
}

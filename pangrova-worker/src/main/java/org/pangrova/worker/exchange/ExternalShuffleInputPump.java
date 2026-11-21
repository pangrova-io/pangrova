package org.pangrova.worker.exchange;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/** Actual runnable ownership: cancellation of a Future is not an exit witness. */
final class ExternalShuffleInputPump implements Runnable {

    private final Runnable work;
    private final AtomicInteger state = new AtomicInteger(); // queued, running, exited
    private final CountDownLatch exited = new CountDownLatch(1);
    private volatile Thread runner;

    ExternalShuffleInputPump(Runnable work) {
        this.work = work;
    }

    @Override
    public void run() {
        if (!state.compareAndSet(0, 1)) return;
        runner = Thread.currentThread();
        try {
            work.run();
        } finally {
            runner = null;
            state.set(2);
            exited.countDown();
        }
    }

    void cancel() {
        if (state.compareAndSet(0, 2)) {
            // A later executor dequeue cannot enter work after this CAS.
            exited.countDown();
            return;
        }
        Thread active = runner;
        if (active != null) active.interrupt();
    }

    void awaitExit(long deadlineNanos) throws InterruptedException {
        if (runner == Thread.currentThread()) {
            throw new IllegalStateException("An exchange input pump cannot join itself");
        }
        long remaining = Math.max(0, deadlineNanos - System.nanoTime());
        if (!exited.await(remaining, TimeUnit.NANOSECONDS)) {
            throw new IllegalStateException("External shuffle input pump has not exited");
        }
    }
}

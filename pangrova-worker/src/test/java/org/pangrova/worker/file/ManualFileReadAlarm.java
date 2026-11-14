package org.pangrova.worker.file;

import java.util.concurrent.Delayed;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/** A manually fired scheduler: no timer thread or wall-clock waits in deadline race tests. */
final class ManualFileReadAlarm extends ScheduledThreadPoolExecutor {
    private Alarm alarm;
    private Runnable beforeReturn = () -> { };

    ManualFileReadAlarm() { super(1); }

    void beforeReturn(Runnable action) { beforeReturn = action; }

    @Override
    public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
        if (isShutdown()) throw new RejectedExecutionException("Alarm owner stopped");
        alarm = new Alarm(command, unit.toMillis(delay));
        beforeReturn.run();
        return alarm;
    }

    void fire() { alarm.run(); }
    boolean cancelled() { return alarm.isCancelled(); }
    long delayMillis() { return alarm.delayMillis; }

    private static final class Alarm extends FutureTask<Void> implements ScheduledFuture<Void> {
        private final long delayMillis;

        Alarm(Runnable command, long delayMillis) {
            super(command, null);
            this.delayMillis = delayMillis;
        }

        @Override public long getDelay(TimeUnit unit) { return unit.convert(delayMillis, TimeUnit.MILLISECONDS); }
        @Override public int compareTo(Delayed other) { return Long.compare(getDelay(TimeUnit.MILLISECONDS),
                other.getDelay(TimeUnit.MILLISECONDS)); }
    }
}

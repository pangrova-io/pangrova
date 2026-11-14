package org.pangrova.operations.ddl;

import org.pangrova.common.annotation.ExperimentalImplementation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Routes DDL jobs by type: sync for metadata-only, async for data-rewrite.
 *
 * <p>Synchronous execution path works end-to-end. The asynchronous path
 * (DATA_REWRITE jobs) submits work to a single-threaded background executor
 * and updates job state to COMPLETED or FAILED when the work finishes.</p>
 */
@ExperimentalImplementation(trackingIssue = "https://github.com/pangrova/pangrova/issues/F-OPS-002-async-ddl")
public final class AsyncDdlExecutor implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(AsyncDdlExecutor.class);

    /** Grace period (seconds) to await in-flight async DDL jobs before forcing shutdown on close. */
    private static final long DEFAULT_SHUTDOWN_GRACE_SECONDS = 30L;

    private final DdlJobStore store;
    private final DdlExecutionCallback executionCallback;
    private final ExecutorService backgroundExecutor;
    /** Includes accepted queued and executing jobs until their callback exits. */
    private final Set<String> asynchronousJobs = ConcurrentHashMap.newKeySet();
    private volatile long shutdownGraceSeconds = DEFAULT_SHUTDOWN_GRACE_SECONDS;

    /**
     * @param store             persistence for DDL job state
     * @param executionCallback callback that performs the actual DDL work for synchronous jobs
     */
    public AsyncDdlExecutor(DdlJobStore store, DdlExecutionCallback executionCallback) {
        this(store, executionCallback, Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "async-ddl-executor");
            t.setDaemon(true);
            return t;
        }));
    }

    AsyncDdlExecutor(DdlJobStore store, DdlExecutionCallback executionCallback,
                     ExecutorService backgroundExecutor) {
        this.store = Objects.requireNonNull(store, "store");
        this.executionCallback = Objects.requireNonNull(executionCallback, "executionCallback");
        this.backgroundExecutor = Objects.requireNonNull(backgroundExecutor, "backgroundExecutor");
    }

    public DdlJob submit(DdlJob job) {
        if (job.state() != DdlJob.DdlJobState.PENDING) {
            throw new IllegalArgumentException("New DDL jobs must be PENDING");
        }
        store.insert(job);
        if (!store.compareAndUpdateState(job.jobId(), DdlJob.DdlJobState.PENDING, DdlJob.DdlJobState.RUNNING)) {
            return current(job.jobId());
        }
        return routeByType(job);
    }

    /** Cancels queued execution and fences completion. A callback already in flight may finish its side effects. */
    public void cancel(String jobId) {
        if (store.compareAndUpdateState(jobId, DdlJob.DdlJobState.RUNNING, DdlJob.DdlJobState.CANCELLED)) {
            LOG.info("event=ddl_job_cancelled jobId={}", jobId);
        }
    }

    public List<DdlJob> listJobs() {
        return store.findAll();
    }

    /** Overrides the shutdown grace period (seconds) used by {@link #close()}. */
    void setShutdownGraceSeconds(long seconds) {
        this.shutdownGraceSeconds = seconds;
    }

    @Override
    public void close() {
        // F-OPS-001: await in-flight DATA_REWRITE jobs before terminating so a graceful
        // shutdown does not interrupt/abandon work mid-rewrite. Fall back to a forceful
        // shutdown only if the grace window is exceeded.
        backgroundExecutor.shutdown();
        try {
            if (!backgroundExecutor.awaitTermination(shutdownGraceSeconds, TimeUnit.SECONDS)) {
                LOG.warn("event=async_ddl_shutdown_timeout graceSeconds={} action=shutdownNow",
                        shutdownGraceSeconds);
                stopOutstandingJobs();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            stopOutstandingJobs();
        }
    }

    private void stopOutstandingJobs() {
        RuntimeException failure = null;
        try {
            for (String jobId : asynchronousJobs) {
                try {
                    cancel(jobId);
                } catch (RuntimeException error) {
                    if (failure == null) {
                        failure = error;
                    } else if (failure != error) {
                        failure.addSuppressed(error);
                    }
                }
            }
        } finally {
            backgroundExecutor.shutdownNow();
            asynchronousJobs.clear();
        }
        if (failure != null) {
            throw failure;
        }
    }

    private DdlJob current(String jobId) {
        return store.findById(jobId).orElseThrow(() -> new IllegalStateException("Missing DDL job " + jobId));
    }

    private DdlJob routeByType(DdlJob job) {
        return switch (job.type()) {
            case METADATA_ONLY -> executeSynchronously(job);
            case DATA_REWRITE -> scheduleAsynchronously(job);
            case TABLE_LOCK -> executeSynchronously(job);
        };
    }

    private DdlJob executeSynchronously(DdlJob job) {
        LOG.info("event=ddl_executing jobId={} type={}", job.jobId(), job.type());
        try {
            executionCallback.execute(job);
        } catch (RuntimeException failure) {
            LOG.error("event=ddl_execution_failed jobId={}", job.jobId(), failure);
            recordFailure(job, failure);
            throw failure;
        }
        store.compareAndUpdateState(job.jobId(), DdlJob.DdlJobState.RUNNING, DdlJob.DdlJobState.COMPLETED);
        return current(job.jobId());
    }

    private void recordFailure(DdlJob job, RuntimeException failure) {
        try {
            store.compareAndUpdateState(job.jobId(), DdlJob.DdlJobState.RUNNING, DdlJob.DdlJobState.FAILED);
        } catch (RuntimeException storeFailure) {
            if (failure != storeFailure) {
                failure.addSuppressed(storeFailure);
            }
        }
    }

    private DdlJob scheduleAsynchronously(DdlJob job) {
        asynchronousJobs.add(job.jobId());
        try {
            backgroundExecutor.submit(() -> {
                try {
                    if (current(job.jobId()).state() == DdlJob.DdlJobState.RUNNING) {
                        executeSynchronously(job);
                    }
                } finally {
                    asynchronousJobs.remove(job.jobId());
                }
            });
        } catch (RejectedExecutionException failure) {
            asynchronousJobs.remove(job.jobId());
            recordFailure(job, failure);
            throw failure;
        }
        return job.withState(DdlJob.DdlJobState.RUNNING);
    }

    /**
     * Callback for performing the actual DDL work during synchronous execution.
     * Implementations receive the job descriptor and perform the DDL operation
     * (e.g., metadata update, schema change). Exceptions are propagated and
     * cause the job to transition to FAILED state.
     */
    @FunctionalInterface
    public interface DdlExecutionCallback {
        void execute(DdlJob job);
    }
}

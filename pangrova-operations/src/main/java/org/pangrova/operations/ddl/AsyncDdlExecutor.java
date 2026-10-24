package org.pangrova.operations.ddl;

import org.pangrova.common.annotation.Experimental;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Routes DDL jobs by type: sync for metadata-only, async for data-rewrite.
 *
 * <p>Synchronous execution path works end-to-end. The asynchronous path
 * (DATA_REWRITE jobs) submits work to a single-threaded background executor
 * and updates job state to COMPLETED or FAILED when the work finishes.</p>
 */
@Experimental(trackingIssue = "https://github.com/pangrova/pangrova/issues/F-OPS-002-async-ddl")
public final class AsyncDdlExecutor implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(AsyncDdlExecutor.class);

    /** Grace period (seconds) to await in-flight async DDL jobs before forcing shutdown on close. */
    private static final long DEFAULT_SHUTDOWN_GRACE_SECONDS = 30L;

    private final DdlJobStore store;
    private final DdlExecutionCallback executionCallback;
    private final ExecutorService backgroundExecutor;
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
        store.save(job);
        return routeByType(job);
    }

    public void cancel(String jobId) {
        store.findById(jobId).ifPresent(this::tryCancelJob);
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
                backgroundExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            backgroundExecutor.shutdownNow();
        }
    }

    private DdlJob routeByType(DdlJob job) {
        return switch (job.type()) {
            case METADATA_ONLY -> executeSynchronously(job);
            case DATA_REWRITE -> scheduleAsynchronously(job);
            case TABLE_LOCK -> executeSynchronously(job);
        };
    }

    private DdlJob executeSynchronously(DdlJob job) {
        LOG.info("event=ddl_sync_executing jobId={} type={}", job.jobId(), job.type());
        store.updateState(job.jobId(), DdlJob.DdlJobState.RUNNING);
        try {
            executionCallback.execute(job);
            store.updateState(job.jobId(), DdlJob.DdlJobState.COMPLETED);
            return job.withState(DdlJob.DdlJobState.COMPLETED);
        } catch (RuntimeException e) {
            LOG.error("event=ddl_execution_failed jobId={} error={}", job.jobId(), e.getMessage());
            store.updateState(job.jobId(), DdlJob.DdlJobState.FAILED);
            throw e;
        }
    }

    private DdlJob scheduleAsynchronously(DdlJob job) {
        LOG.info("event=ddl_async_scheduled jobId={} type={}", job.jobId(), job.type());
        store.updateState(job.jobId(), DdlJob.DdlJobState.RUNNING);
        backgroundExecutor.submit(() -> {
            try {
                executionCallback.execute(job);
                store.updateState(job.jobId(), DdlJob.DdlJobState.COMPLETED);
                LOG.info("event=ddl_async_completed jobId={}", job.jobId());
            } catch (RuntimeException e) {
                LOG.error("event=ddl_async_failed jobId={} error={}", job.jobId(), e.getMessage());
                store.updateState(job.jobId(), DdlJob.DdlJobState.FAILED);
            }
        });
        return job.withState(DdlJob.DdlJobState.RUNNING);
    }

    private void tryCancelJob(DdlJob job) {
        if (store.compareAndUpdateState(job.jobId(), DdlJob.DdlJobState.RUNNING,
                DdlJob.DdlJobState.CANCELLED)) {
            LOG.info("event=ddl_job_cancelled jobId={}", job.jobId());
        } else {
            LOG.debug("event=ddl_cancel_skipped jobId={} reason=not_running", job.jobId());
        }
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

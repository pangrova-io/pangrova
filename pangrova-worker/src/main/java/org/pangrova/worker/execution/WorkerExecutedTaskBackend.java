package org.pangrova.worker.execution;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.protocol.v2.TaskKind;
import org.pangrova.protocol.v2.TaskRequest;

import java.util.Optional;

/**
 * Executes canonical tasks in the Worker JVM instead of the Native engine.
 *
 * <p>A backend owns its positive handle namespace and must return a canonical
 * terminal report from {@link #release}. It never reports Native statistics and
 * never publishes a target catalog; the Coordinator remains the publication owner.</p>
 */
@Experimental
public interface WorkerExecutedTaskBackend extends AutoCloseable {

    boolean supports(TaskKind taskKind);

    /** Creates one backend-owned task; the returned handle must be positive. */
    long create(TaskRequest request);

    void start(long handle);

    boolean isFinished(long handle);

    /** Waits for the Worker task's terminal report to become available. */
    boolean awaitFinished(long handle, int waitMillis) throws InterruptedException;

    /** Returns the actual canonical terminal candidate; never a synthetic success. */
    byte[] getTaskStatus(long handle);

    /** Optional JVM telemetry; absent/failed statistics must not be fabricated. */
    default byte[] getTaskStats(long handle) {
        throw new UnsupportedOperationException("Worker-executed backend exposes no Native statistics");
    }

    void cancel(long handle);

    /** Returns every commit fragment staged by the completed task. */
    java.util.List<byte[]> getCommitFragments(long handle);

    /** Releases the backend handle and returns its exact terminal report when still owned. */
    Optional<byte[]> release(long handle);

    @Override
    void close();
}

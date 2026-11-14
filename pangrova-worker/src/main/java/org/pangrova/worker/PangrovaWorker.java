package org.pangrova.worker;

import static org.pangrova.common.util.Preconditions.checkNotNull;
import static org.pangrova.common.util.Preconditions.checkState;

import org.pangrova.worker.task.TaskManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Main entry point for the Pangrova Worker process.
 * Implements 4-Phase startup and reverse-order shutdown per architecture doc 01 §2.4.
 *
 * <p>Transport and exchange are now fully native (C++ NativeResultServer and
 * NativeExchangeTransport). Java PangrovaWorker only manages TaskManager lifecycle.</p>
 */
public class PangrovaWorker {

    private static final Logger LOG = LoggerFactory.getLogger(PangrovaWorker.class);

    private final TaskManager taskManager;
    private final AtomicBoolean running = new AtomicBoolean(false);

    public PangrovaWorker(WorkerComponents components) {
        checkNotNull(components, "components must not be null");
        this.taskManager = components.taskManager();
    }

    /**
     * Starts the worker using 4-Phase startup sequence.
     * Phase 1: Native engine init (static initializer, no-op in Phase 0)
     * Phase 2: Java layer init (TaskManager)
     * Phase 3: Network services (native Flight servers handle transport)
     * Phase 4: Cluster registration (deferred to production wiring)
     */
    public void start() {
        checkState(!running.get(), "Worker is already running");
        LOG.info("event=worker_start phase=1_native");
        taskManager.initialize();
        LOG.info("event=worker_start phase=2_java_layer");
        LOG.info("event=worker_start phase=3_network_native");
        LOG.info("event=worker_start phase=4_registration_deferred");
        running.set(true);
        LOG.info("event=worker_started");
    }

    /**
     * Stops the worker in reverse phase order.
     * Step 1: Drain tasks (Phase 2 reverse)
     * Step 2: Native transport shutdown handled by bootstrap
     */
    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        LOG.info("event=worker_stop phase=4_reverse_deregistration_deferred");
        taskManager.drainAndWait();
        LOG.info("event=worker_stop phase=2_reverse_tasks_drained");
        LOG.info("event=worker_stop phase=3_reverse_network_native");
        LOG.info("event=worker_stop phase=1_reverse_native_deferred");
        LOG.info("event=worker_stopped");
    }

    public boolean isRunning() {
        return running.get();
    }

    public TaskManager getTaskManager() {
        return taskManager;
    }
}

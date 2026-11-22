package org.pangrova.worker.bootstrap;

import org.pangrova.worker.enums.WorkerBootstrapState;

import org.pangrova.worker.bootstrap.ownership.WorkerComponentRefs;

import java.util.concurrent.atomic.AtomicBoolean;

import org.pangrova.common.concurrent.GuardedBy;
import org.pangrova.common.concurrent.ThreadSafe;

/** Serializes one-shot bootstrap admission without holding a lock during I/O. */
@ThreadSafe
final class WorkerBootstrapLifecycle {

    @GuardedBy("this")
    private WorkerBootstrapState state = WorkerBootstrapState.NEW;

    private final AtomicBoolean ready = new AtomicBoolean();

    AtomicBoolean readyFlag() {
        return ready;
    }

    synchronized void beginStart() {
        require(WorkerBootstrapState.NEW, "WorkerBootstrap cannot start in state ");
        state = WorkerBootstrapState.STARTING;
    }

    synchronized void started() {
        require(WorkerBootstrapState.STARTING, "WorkerBootstrap startup is not admitted in state ");
        ready.set(true);
        state = WorkerBootstrapState.RUNNING;
    }

    synchronized boolean beginShutdown(WorkerComponentRefs refs) {
        if (state == WorkerBootstrapState.STOPPED) return false;
        if (state == WorkerBootstrapState.NEW
                && refs.cdcConsumer() == null
                && refs.workerExecutedTaskBackend() == null
                && !(refs.catalogResourceResolver() instanceof AutoCloseable)) return false;
        if (state != WorkerBootstrapState.NEW && state != WorkerBootstrapState.RUNNING
                && state != WorkerBootstrapState.CLEANUP_PENDING) {
            throw new IllegalStateException("WorkerBootstrap cannot shutdown in state " + state);
        }
        ready.set(false);
        state = WorkerBootstrapState.STOPPING;
        return true;
    }

    synchronized void cleaned(boolean success) {
        if (state != WorkerBootstrapState.STARTING && state != WorkerBootstrapState.STOPPING) {
            throw new IllegalStateException(
                    "WorkerBootstrap cleanup is not admitted in state " + state);
        }
        ready.set(false);
        state = success ? WorkerBootstrapState.STOPPED : WorkerBootstrapState.CLEANUP_PENDING;
    }

    synchronized void beginConfiguration() {
        require(WorkerBootstrapState.NEW, "WorkerBootstrap cannot configure in state ");
        state = WorkerBootstrapState.CONFIGURING;
    }

    synchronized void configured() {
        require(WorkerBootstrapState.CONFIGURING, "WorkerBootstrap configuration is not admitted in state ");
        state = WorkerBootstrapState.NEW;
    }

    private void require(WorkerBootstrapState expected, String message) {
        if (state != expected) throw new IllegalStateException(message + state);
    }
}

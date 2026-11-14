package org.pangrova.worker.bootstrap;

import org.pangrova.worker.bootstrap.ownership.WorkerComponentRefs;

import java.util.concurrent.atomic.AtomicBoolean;

import org.pangrova.common.concurrent.GuardedBy;
import org.pangrova.common.concurrent.ThreadSafe;

/** Serializes one-shot bootstrap admission without holding a lock during I/O. */
@ThreadSafe
final class WorkerBootstrapLifecycle {
    private enum State {
        NEW,
        CONFIGURING,
        STARTING,
        RUNNING,
        STOPPING,
        CLEANUP_PENDING,
        STOPPED
    }

    @GuardedBy("this")
    private State state = State.NEW;

    private final AtomicBoolean ready = new AtomicBoolean();

    AtomicBoolean readyFlag() {
        return ready;
    }

    synchronized void beginStart() {
        require(State.NEW, "WorkerBootstrap cannot start in state ");
        state = State.STARTING;
    }

    synchronized void started() {
        require(State.STARTING, "WorkerBootstrap startup is not admitted in state ");
        ready.set(true);
        state = State.RUNNING;
    }

    synchronized boolean beginShutdown(WorkerComponentRefs refs) {
        if (state == State.STOPPED) return false;
        if (state == State.NEW
                && refs.cdcConsumer() == null
                && !(refs.catalogResourceResolver() instanceof AutoCloseable)) return false;
        if (state != State.NEW && state != State.RUNNING && state != State.CLEANUP_PENDING) {
            throw new IllegalStateException("WorkerBootstrap cannot shutdown in state " + state);
        }
        ready.set(false);
        state = State.STOPPING;
        return true;
    }

    synchronized void cleaned(boolean success) {
        if (state != State.STARTING && state != State.STOPPING) {
            throw new IllegalStateException(
                    "WorkerBootstrap cleanup is not admitted in state " + state);
        }
        ready.set(false);
        state = success ? State.STOPPED : State.CLEANUP_PENDING;
    }

    synchronized void beginConfiguration() {
        require(State.NEW, "WorkerBootstrap cannot configure in state ");
        state = State.CONFIGURING;
    }

    synchronized void configured() {
        require(State.CONFIGURING, "WorkerBootstrap configuration is not admitted in state ");
        state = State.NEW;
    }

    private void require(State expected, String message) {
        if (state != expected) throw new IllegalStateException(message + state);
    }
}

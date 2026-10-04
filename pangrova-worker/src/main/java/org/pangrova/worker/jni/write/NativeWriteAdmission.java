package org.pangrova.worker.jni.write;

import org.pangrova.common.concurrent.ThreadSafe;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/** Short ownership claims only: connector calls and cleanup never run under this monitor. */
@ThreadSafe
final class NativeWriteAdmission {
    private final Map<String, Integer> inFlight = new HashMap<>();
    private final Set<String> releasing = new HashSet<>();
    private boolean sealed;
    private boolean cleaning;

    synchronized void enter(String taskId) {
        requireTaskId(taskId);
        requireOpen();
        if (cleaning || releasing.contains(taskId)) {
            throw new IllegalStateException("Write admission cleanup is in progress");
        }
        inFlight.merge(taskId, 1, Math::addExact);
    }

    synchronized void leave(String taskId) {
        int count = inFlight.getOrDefault(taskId, 0);
        if (count == 0) throw new IllegalStateException("Write admission has no active owner");
        if (count == 1) inFlight.remove(taskId);
        else inFlight.put(taskId, count - 1);
    }

    synchronized void requireOpen() {
        if (sealed) throw new IllegalStateException("Native write executor is closed");
    }

    synchronized void beginRelease(String taskId) {
        requireTaskId(taskId);
        if (cleaning || inFlight.containsKey(taskId) || !releasing.add(taskId)) {
            throw new IllegalStateException("Task write admission or cleanup is still in progress");
        }
    }

    synchronized void endRelease(String taskId) {
        releasing.remove(taskId);
    }

    synchronized void beginCleanup(boolean seal) {
        if (seal) sealed = true;
        if (cleaning || !inFlight.isEmpty() || !releasing.isEmpty()) {
            throw new IllegalStateException("Write admission or cleanup is still in progress");
        }
        cleaning = true;
    }

    synchronized void endCleanup() {
        cleaning = false;
    }

    private static void requireTaskId(String taskId) {
        if (taskId == null || taskId.isBlank()) {
            throw new IllegalArgumentException("Canonical taskId is required for write admission");
        }
    }
}

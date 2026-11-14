package org.pangrova.worker.task;

import org.pangrova.worker.exception.TaskAdmissionException;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/** Owns local task IDs and live execution records. Canonical admission receipts outlive this registry. */
final class WorkerTaskRegistry {
    private final int capacity;
    private volatile boolean admissionsClosed;
    private final AtomicLong sequence = new AtomicLong();
    private final ConcurrentHashMap<String, TaskInfo> owners = new ConcurrentHashMap<>();
    private final Map<String, TaskInfo> view = Collections.unmodifiableMap(owners);

    WorkerTaskRegistry(int capacity) {
        if (capacity < 1) throw new IllegalArgumentException("Task tracking capacity must be positive");
        this.capacity = capacity;
    }

    String nextId(String queryId) {
        return queryId + "-" + sequence.incrementAndGet();
    }

    synchronized void closeAdmissions() {
        admissionsClosed = true;
    }

    void requireCapacity() {
        if (admissionsClosed) {
            throw new TaskAdmissionException(TaskAdmissionException.Reason.CLOSED, "Worker is draining");
        }
        if (owners.size() >= capacity) {
            throw new TaskAdmissionException(TaskAdmissionException.Reason.CAPACITY,
                    "Task resource tracking limit reached: " + capacity);
        }
    }

    synchronized void register(TaskInfo info) {
        requireCapacity();
        insert(info);
    }

    /** A failed rollback must stay reachable even if another admission took the last free slot. */
    synchronized void retainForCleanup(TaskInfo info) {
        insert(info);
    }

    private void insert(TaskInfo info) {
        TaskInfo previous = owners.putIfAbsent(info.taskId(), info);
        if (previous != null && previous != info) {
            throw new IllegalStateException("Task resource owner already exists: " + info.taskId());
        }
    }

    boolean remove(TaskInfo info) {
        return owners.remove(info.taskId(), info);
    }

    TaskInfo get(String taskId) { return owners.get(taskId); }
    Collection<TaskInfo> values() { return view.values(); }
    Set<Map.Entry<String, TaskInfo>> entries() { return view.entrySet(); }
}

package org.pangrova.query.recovery;

/** Task-owner registration applied while replaying durable journal state. */
public interface RecoveredTaskRegistry {

    void registerTask(String queryId, String taskId);

    void markScheduled(String queryId, String taskId);

    void assignWorker(String taskId, String workerId);

    boolean updateTaskState(String queryId, String taskId, String workerState);
}

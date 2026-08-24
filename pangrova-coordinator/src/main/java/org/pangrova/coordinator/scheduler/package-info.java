/**
 * Coordinator-side scheduling: worker selection, task dispatch, and stage ordering.
 *
 * <h2>Package structure</h2>
 * <ul>
 *   <li>{@link org.pangrova.coordinator.scheduler.TaskDispatcher} — dispatch contract
 *       (interface); {@link org.pangrova.coordinator.scheduler.RemoteTaskDispatcher}
 *       is the HTTP/REST implementation.</li>
 *   <li>{@link org.pangrova.coordinator.scheduler.TaskScheduler} — selects the best
 *       worker for a task based on memory pressure, performance score, data locality,
 *       rack awareness, and optional weighted fair scheduling.</li>
 *   <li>{@link org.pangrova.coordinator.scheduler.StageScheduler} — topological
 *       ordering of stage DAGs (leaf-first, output-last).</li>
 *   <li>{@code scheduler/strategy/} — pluggable scheduling strategies
 *       (e.g. {@link org.pangrova.coordinator.scheduler.strategy.FairScheduler}).</li>
 * </ul>
 *
 * <h2>Event-driven scheduling (G-SCHED-08)</h2>
 *
 * <p>The coordinator's query control loop is event-driven: stage lifecycle events
 * (started, finished, failed) drive downstream scheduling and query completion.
 * The {@link org.pangrova.coordinator.scheduler.SchedulerEventLoop} interface
 * models this contract.</p>
 *
 * <p><b>Migration history:</b> Prior to G-SCHED-08, query completion detection
 * used a thread-per-query busy-poll pattern: when the task tracker reported all
 * tasks finished but the DAG engine had not yet completed all stages (async
 * dispatch race), a daemon thread polled {@code isQueryComplete()} every 50ms.
 * This wasted threads (O(N) where N = concurrent deferred queries) and added
 * polling latency. The fix replaced polling with a {@code CompletableFuture}
 * callback registered on the DAG engine's per-query completion signal — O(1)
 * per deferred query, zero polling threads.</p>
 *
 * <p><b>Dead code removed:</b> The {@code OutputWorkerSelector} class and its
 * warm-pool mechanism were deleted (G-SCHED-08). The warm pool was never wired
 * into the actual dispatch path ({@link org.pangrova.coordinator.scheduler.TaskScheduler}
 * + {@link org.pangrova.coordinator.query.execution.dispatch.dag.DefaultWorkerSelector}
 * handle all real worker selection). Cache-locality scheduling now uses
 * {@code CacheAffinityAssigner} (consistent hashing) and
 * {@code DefaultWorkerSelector.selectByCachedFiles} (heartbeat-reported cache state).</p>
 */
package org.pangrova.coordinator.scheduler;

package org.pangrova.coordinator.generation.runtime;

import org.pangrova.execution.task.DispatchResult;
import org.pangrova.coordinator.query.execution.dispatch.dag.DAGDispatchEngine;
import org.pangrova.coordinator.query.execution.dispatch.dag.control.DurableStageRuntime;
import org.pangrova.generation.api.identity.GenerationJobKey;
import org.pangrova.persistence.generation.api.job.GenerationJobStore;
import org.pangrova.generation.api.graph.GenerationStageGraph;
import org.pangrova.common.api.annotation.Internal;
import org.pangrova.execution.api.ownership.ExecutionAnchor;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/** Reconciles durable relational jobs through the existing shared DAG engine, including blocked-barrier restart. */
@Internal
public final class DurableGenerationGraphs {
    private final Dependencies dependencies;
    private final Clock clock;
    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(DurableGenerationGraphs.class);
    private final ConcurrentHashMap<GenerationJobKey, CompletableFuture<DispatchResult>> active =
            new ConcurrentHashMap<>();
    private final ConcurrentHashMap<GenerationJobKey, Instant> retryAfter = new ConcurrentHashMap<>();

    /** Shares the same Worker scheduler, trusted partition gate and ordinary dispatch engine. */
    public record Dependencies(GenerationJobStore jobs, DurableStageRuntime.Dependencies runtime,
            DAGDispatchEngine engine, Duration retryBackoff) {
        public Dependencies {
            java.util.Objects.requireNonNull(jobs, "jobs");
            java.util.Objects.requireNonNull(runtime, "runtime");
            java.util.Objects.requireNonNull(engine, "engine");
            if (retryBackoff == null || retryBackoff.isZero() || retryBackoff.isNegative()) {
                throw new IllegalArgumentException("Graph retry backoff must be positive");
            }
        }
    }

    public DurableGenerationGraphs(Dependencies dependencies, Clock clock) {
        this.dependencies = java.util.Objects.requireNonNull(dependencies, "dependencies");
        this.clock = java.util.Objects.requireNonNull(clock, "clock");
    }

    /** Routing metadata is not execution authority; the same-PG preparation below supplies that capability. */
    public boolean resume(GenerationJobKey key, ExecutionAnchor anchor) {
        var spec = dependencies.jobs().findDispatchSpec(key.tenantId(), key.jobId()).orElseThrow();
        if (spec.stageGraph().purpose() != GenerationStageGraph.Purpose.RELATIONAL_EXPORT
                || spec.stageGraph().exchanges().isEmpty()) {
            return false;
        }
        if (active.containsKey(key) || clock.instant().isBefore(retryAfter.getOrDefault(key, Instant.MIN))) {
            return true;
        }
        var pending = new CompletableFuture<DispatchResult>();
        if (active.putIfAbsent(key, pending) != null) return true;
        pending.whenComplete((result, failure) -> {
            if (failure != null) {
                retryAfter.put(key, clock.instant().plus(dependencies.retryBackoff()));
                LOG.warn("event=durable_graph_pass_failed executionId={}", key.executionId(), failure);
            }
            active.remove(key, pending);
        });
        startPass(key, anchor, pending);
        return true;
    }

    private void startPass(GenerationJobKey key, ExecutionAnchor anchor,
            CompletableFuture<DispatchResult> pending) {
        try {
            var prepared = dependencies.runtime().stages().prepareExecution(key, anchor);
            dependencies.engine().startDurable(new DurableStageRuntime(prepared, dependencies.runtime()))
                    .whenComplete((result, failure) -> {
                        if (failure == null) pending.complete(result);
                        else pending.completeExceptionally(failure);
                    });
        } catch (RuntimeException failure) {
            pending.completeExceptionally(failure);
            throw failure;
        }
    }

    /** Publication may yield the execution anchor only after the ordinary DAG settles its complete graph. */
    public boolean isActive(GenerationJobKey key) { return active.containsKey(key); }

    /** Stops control and actual tasks without deleting durable external shuffle data required by recovery. */
    public void cancel(GenerationJobKey key) {
        dependencies.engine().cancelDurable(key.executionId());
        retryAfter.remove(key);
    }
}

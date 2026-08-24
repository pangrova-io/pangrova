package org.pangrova.coordinator.bootstrap.module;

import org.pangrova.coordinator.query.execution.phase.CompilationPhase;
import org.pangrova.coordinator.query.execution.phase.DistributedDispatchPhase;
import org.pangrova.coordinator.query.execution.phase.StatementDispatchPhase;
import org.pangrova.coordinator.scheduler.TaskScheduler;
import org.pangrova.coordinator.worker.WorkerPerformanceScorer;
import org.pangrova.resource.scheduler.WeightedFairScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Core query execution module (always loaded, priority 0).
 * Binds the task scheduler and adds the fundamental pipeline phases:
 * StatementDispatch, Compilation, and DistributedDispatch.
 *
 * <p>This module expects the following bindings to be pre-populated
 * by the bootstrap before module loading begins:
 * {@code CompilationPhase}, {@code DistributedDispatchPhase},
 * {@code StatementDispatchPhase}.</p>
 */
public final class QueryExecutionModule implements CoordinatorModule {

    private static final Logger LOG = LoggerFactory.getLogger(QueryExecutionModule.class);

    private final boolean tenantAwareScheduling;

    public QueryExecutionModule(boolean tenantAwareScheduling) {
        this.tenantAwareScheduling = tenantAwareScheduling;
    }

    @Override
    public void configure(CoordinatorModuleContext context) {
        TaskScheduler scheduler = buildTaskScheduler();
        context.bind(TaskScheduler.class, scheduler);

        StatementDispatchPhase statementPhase = context.optional(StatementDispatchPhase.class);
        if (statementPhase != null) {
            context.addQueryPhase(statementPhase);
        }

        CompilationPhase compilationPhase = context.optional(CompilationPhase.class);
        if (compilationPhase != null) {
            context.addQueryPhase(compilationPhase);
        }

        DistributedDispatchPhase dispatchPhase = context.optional(DistributedDispatchPhase.class);
        if (dispatchPhase != null) {
            context.addQueryPhase(dispatchPhase);
        }

        LOG.info("event=module_configured module=QueryExecution tenantScheduling={}",
                tenantAwareScheduling);
    }

    @Override
    public int priority() {
        return 0;
    }

    private TaskScheduler buildTaskScheduler() {
        WeightedFairScheduler fairScheduler = tenantAwareScheduling
                ? new WeightedFairScheduler() : null;
        return new TaskScheduler(new WorkerPerformanceScorer(), fairScheduler);
    }
}

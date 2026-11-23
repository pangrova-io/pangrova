package org.pangrova.coordinator.query.execution.dispatch;
import org.pangrova.query.dispatch.StatementResult;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;

import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.parser.api.ast.Statement;
import org.pangrova.query.dispatch.StatementExecutionContext;
import org.pangrova.query.dispatch.StatementExecutionFactory;

/**
 * Consolidated factory for direct metadata operations (DDL, SHOW, INSERT VALUES,
 * maintenance, index, clustering, tenant, stream, dynamic table).
 *
 * <p>Holds a {@code Map<Class, MetadataTask>} registry. Each statement type maps
 * to exactly one {@link MetadataTask} implementation. New statement types only
 * need a task implementation registered — no new factory class.
 *
 * <p>Thread-safe: uses {@link ConcurrentHashMap} for post-construction registration
 * via {@link #registerTask(Class, MetadataTask)}.
 */
public final class MetadataExecutionFactory implements StatementExecutionFactory<Statement> {

    private final Map<Class<? extends Statement>, MetadataTask<?>> tasks;

    public MetadataExecutionFactory(Map<Class<? extends Statement>, MetadataTask<?>> tasks) {
        this.tasks = new ConcurrentHashMap<>(tasks);
    }

    @Override
    public Set<Class<? extends Statement>> supportedTypes() {
        return tasks.keySet();
    }

    @Override
    public StatementResult execute(Statement statement, StatementExecutionContext context) {
        return task(statement).execute(statement, context);
    }

    @Override
    public CompletionStage<StatementResult> start(Statement statement, StatementExecutionContext context) {
        return task(statement).start(statement, context);
    }

    @SuppressWarnings("unchecked")
    private MetadataTask<Statement> task(Statement statement) {
        var task = (MetadataTask<Statement>) tasks.get(statement.getClass());
        if (task == null) {
            throw new PangrovaException(PangrovaErrorCode.PLANNER_VALIDATION_FAILURE,
                    "No metadata task registered for: " + statement.getClass().getSimpleName());
        }
        return task;
    }

    /** Registers an additional task after construction (for dynamic handlers). */
    public <T extends Statement> void registerTask(Class<T> type, MetadataTask<T> task) {
        tasks.put(type, task);
    }
}

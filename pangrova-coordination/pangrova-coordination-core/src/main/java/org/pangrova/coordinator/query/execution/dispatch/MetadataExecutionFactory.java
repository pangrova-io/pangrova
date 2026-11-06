package org.pangrova.coordinator.query.execution.dispatch;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.parser.api.ast.Statement;

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
    @SuppressWarnings("unchecked")
    public StatementResult execute(Statement statement, StatementExecutionContext context) {
        MetadataTask<Statement> task = (MetadataTask<Statement>) tasks.get(statement.getClass());
        if (task == null) {
            throw new PangrovaException(PangrovaErrorCode.PLANNER_VALIDATION_FAILURE,
                    "No metadata task registered for: " + statement.getClass().getSimpleName());
        }
        return task.execute(statement, context);
    }

    /** Registers an additional task after construction (for dynamic handlers). */
    public <T extends Statement> void registerTask(Class<T> type, MetadataTask<T> task) {
        tasks.put(type, task);
    }
}

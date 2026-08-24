package org.pangrova.coordinator.bootstrap.module;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.pangrova.coordinator.query.execution.dispatch.StatementCompletionListener;
import org.pangrova.coordinator.query.execution.dispatch.dag.handler.StageLifecycleHandler;
import org.pangrova.coordinator.query.execution.phase.QueryExecutionPhase;

/**
 * Default implementation of {@link CoordinatorModuleContext} that accumulates
 * bindings and pipeline components during module configuration. After all
 * modules have been configured, the assembled components can be retrieved
 * for building the query dispatcher and DAG engine.
 */
public final class DefaultModuleContext implements CoordinatorModuleContext {

    private final Map<Class<?>, Object> bindings = new LinkedHashMap<>();
    private final List<QueryExecutionPhase> queryPhases = new ArrayList<>();
    private final List<StageLifecycleHandler> stageHandlers = new ArrayList<>();
    private final List<StatementCompletionListener> completionListeners = new ArrayList<>();

    @Override
    public <T> void bind(Class<T> type, T instance) {
        if (type == null) {
            throw new IllegalArgumentException("type must not be null");
        }
        if (instance == null) {
            throw new IllegalArgumentException("instance must not be null");
        }
        bindings.put(type, instance);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T require(Class<T> type) {
        T instance = (T) bindings.get(type);
        if (instance == null) {
            throw new IllegalStateException(
                    "Required binding not found: " + type.getName());
        }
        return instance;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T optional(Class<T> type) {
        return (T) bindings.get(type);
    }

    @Override
    public void addQueryPhase(QueryExecutionPhase phase) {
        if (phase == null) {
            throw new IllegalArgumentException("phase must not be null");
        }
        queryPhases.add(phase);
    }

    @Override
    public void addStageHandler(StageLifecycleHandler handler) {
        if (handler == null) {
            throw new IllegalArgumentException("handler must not be null");
        }
        stageHandlers.add(handler);
    }

    @Override
    public void addCompletionListener(StatementCompletionListener listener) {
        if (listener == null) {
            throw new IllegalArgumentException("listener must not be null");
        }
        completionListeners.add(listener);
    }

    /** Returns an unmodifiable view of the registered query phases. */
    public List<QueryExecutionPhase> queryPhases() {
        return Collections.unmodifiableList(queryPhases);
    }

    /** Returns an unmodifiable view of the registered stage handlers. */
    public List<StageLifecycleHandler> stageHandlers() {
        return Collections.unmodifiableList(stageHandlers);
    }

    /** Returns an unmodifiable view of the registered completion listeners. */
    public List<StatementCompletionListener> completionListeners() {
        return Collections.unmodifiableList(completionListeners);
    }
}

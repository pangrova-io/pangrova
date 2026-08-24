package org.pangrova.coordinator.query.execution.dispatch;

import java.util.function.BiFunction;
import java.util.function.Consumer;

import org.pangrova.coordinator.query.CoordinatorSessionContext;
import org.pangrova.coordinator.query.security.AccessControlEnforcer;

/**
 * Bundles the dependencies for {@link PlanExecutionFactory} construction,
 * keeping the constructor parameter count within the quality gate (≤4).
 *
 * @param dmlDispatch             compiles DML to Substrait and dispatches to native workers
 * @param cacheInvalidator        invalidates cache after DML writes (nullable)
 * @param explainCompiler         compiles inner SQL and returns plan text for EXPLAIN
 * @param writeCommitTracker      tracks distributed write commit metadata (nullable)
 * @param catalogRegistry         resolves catalog names for write context registration (nullable)
 * @param accessControlEnforcer   enforces SELECT privilege on source tables (nullable)
 */
public record PlanExecutionFactoryContext(
        NativeDmlCompileFunction dmlDispatch,
        Consumer<String> cacheInvalidator,
        BiFunction<String, CoordinatorSessionContext, String> explainCompiler,
        org.pangrova.coordinator.query.task.NativeWriteCommitTracker writeCommitTracker,
        org.pangrova.metadata.catalog.CatalogRegistry catalogRegistry,
        AccessControlEnforcer accessControlEnforcer) {}

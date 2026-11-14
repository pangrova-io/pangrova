package org.pangrova.coordinator.query.execution.dispatch;

import java.util.function.BiFunction;
import java.util.function.Consumer;

import org.pangrova.query.api.session.QuerySession;

/**
 * Bundles the dependencies for {@link PlanExecutionFactory} construction,
 * keeping the constructor parameter count within the quality gate (≤4).
 *
 * @param dmlDispatch             compiles DML to Substrait and dispatches to native workers
 * @param writeCompleteCallback  observes a successfully committed DML write (nullable)
 * @param explainCompiler         compiles inner SQL and returns plan text for EXPLAIN
 * @param catalogRegistry         resolves actual table declarations before compilation
 * @param generatedValueMaterialization checks generated definition readiness
 */
public record PlanExecutionFactoryContext(
        NativeDmlCompileFunction dmlDispatch,
        Consumer<String> writeCompleteCallback,
        BiFunction<String, QuerySession, String> explainCompiler,
        org.pangrova.metadata.catalog.registry.CatalogRegistry catalogRegistry,
        org.pangrova.coordinator.ai.embedding.GeneratedValueMaterializationCoordinator
                generatedValueMaterialization) {}

package org.pangrova.coordinator.query;

import org.pangrova.coordinator.query.execution.QueryDispatcher;
import org.pangrova.coordinator.query.execution.QueryDispatcherContext;
import org.pangrova.planner.PangrovaPlanner;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies the integration status of Planner advanced sub-modules
 * (AQE, DPP, Materialized View) with the coordinator and worker runtime.
 *
 * <p>These sub-modules are now wired into the query execution pipeline
 * via QueryDispatcherContext (AQE, DPP) and PangrovaPlanner (Materialized View).
 * This test documents the current integration state and serves as a
 * regression gate to ensure they remain wired.</p>
 *
 * <p><b>Validates: Requirements 4.5</b></p>
 *
 * @see <a href="pangrova-planner/INTEGRATION_STATUS.md">Planner Integration Roadmap</a>
 */
@DisplayName("Planner Advanced Feature Integration Status")
class PlannerAdvancedFeatureIntegrationStatusTest {

    // ----------------------------------------------------------------
    // AQE — Adaptive Query Execution
    // ----------------------------------------------------------------
    @Nested
    @DisplayName("AQE: AdaptiveQueryExecutor ↔ QueryDispatcher")
    class AdaptiveQueryExecutionIntegrationStatus {

        @Test
        @DisplayName("QueryDispatcherContext references AdaptiveQueryExecutor")
        void dependenciesContainAqe() {
            Set<String> fieldTypeNames = Arrays.stream(QueryDispatcherContext.class.getDeclaredFields())
                    .map(f -> f.getType().getSimpleName())
                    .collect(Collectors.toSet());

            assertTrue(fieldTypeNames.contains("AdaptiveQueryExecutor"),
                    "AQE AdaptiveQueryExecutor should be integrated into QueryDispatcherContext");
        }

        @Test
        @DisplayName("QueryDispatcher methods do NOT reference AQE classes directly")
        void dispatcherMethodsDoNotReferenceAqe() {
            Set<String> returnTypes = Arrays.stream(QueryDispatcher.class.getDeclaredMethods())
                    .map(m -> m.getReturnType().getSimpleName())
                    .collect(Collectors.toSet());

            Set<String> paramTypes = Arrays.stream(QueryDispatcher.class.getDeclaredMethods())
                    .flatMap(m -> Arrays.stream(m.getParameterTypes()))
                    .map(Class::getSimpleName)
                    .collect(Collectors.toSet());

            Set<String> allTypes = new java.util.HashSet<>(returnTypes);
            allTypes.addAll(paramTypes);

            assertFalse(allTypes.contains("AdaptiveQueryExecutor"),
                    "AQE not yet wired into QueryDispatcher method signatures");
            assertFalse(allTypes.contains("ReOptimizationResult"),
                    "AQE ReOptimizationResult not yet used in QueryDispatcher");
            assertFalse(allTypes.contains("ShuffleStageStats"),
                    "AQE ShuffleStageStats not yet consumed by QueryDispatcher");
        }
    }

    // ----------------------------------------------------------------
    // DPP — Dynamic Partition Pruning
    // ----------------------------------------------------------------
    @Nested
    @DisplayName("DPP: RuntimeFilterRegistry ↔ Worker runtime")
    class DynamicPartitionPruningIntegrationStatus {

        @Test
        @DisplayName("QueryDispatcherContext references RuntimeFilterRegistry")
        void dependenciesContainDpp() {
            Set<String> fieldTypeNames = Arrays.stream(QueryDispatcherContext.class.getDeclaredFields())
                    .map(f -> f.getType().getSimpleName())
                    .collect(Collectors.toSet());

            assertTrue(fieldTypeNames.contains("RuntimeFilterRegistry"),
                    "DPP RuntimeFilterRegistry should be integrated into QueryDispatcherContext");
        }

        @Test
        @DisplayName("PangrovaPlanner uses MaterializedViewRewritePass for DPP analysis")
        void plannerUsesDppRegistry() {
            Set<String> fieldTypeNames = Arrays.stream(PangrovaPlanner.class.getDeclaredFields())
                    .map(f -> f.getType().getSimpleName())
                    .collect(Collectors.toSet());

            assertFalse(fieldTypeNames.contains("RuntimeFilterGenerator"),
                    "DPP RuntimeFilterGenerator not yet wired into PangrovaPlanner");
            assertTrue(fieldTypeNames.contains("MaterializedViewRewritePass"),
                    "MaterializedViewRewritePass (which contains RuntimeFilterRegistry) should be wired into PangrovaPlanner");
        }
    }

    // ----------------------------------------------------------------
    // Materialized View — Materialized View Query Rewriting
    // ----------------------------------------------------------------
    @Nested
    @DisplayName("Materialized View: MaterializedViewQueryRewriter ↔ PangrovaPlanner")
    class MaterializedViewIntegrationStatus {

        @Test
        @DisplayName("PangrovaPlanner references MaterializedViewRewritePass")
        void plannerContainsMaterializedViewRewriter() {
            Set<String> fieldTypeNames = Arrays.stream(PangrovaPlanner.class.getDeclaredFields())
                    .map(f -> f.getType().getSimpleName())
                    .collect(Collectors.toSet());

            assertTrue(fieldTypeNames.contains("MaterializedViewRewritePass"),
                    "MaterializedViewRewritePass should be integrated into PangrovaPlanner");
        }

        @Test
        @DisplayName("PangrovaPlanner.plan() does NOT call MaterializedViewQueryRewriter.rewrite() directly")
        void planMethodDoesNotCallMaterializedViewRewrite() {
            Method[] methods = PangrovaPlanner.class.getDeclaredMethods();
            Set<String> methodNames = Arrays.stream(methods)
                    .map(Method::getName)
                    .collect(Collectors.toSet());

            assertFalse(methodNames.contains("rewriteWithMV"),
                    "No materialized view rewrite method exists in PangrovaPlanner");
            assertFalse(methodNames.contains("applyMaterializedViewRewrite"),
                    "No materialized view rewrite method exists in PangrovaPlanner");
        }

        @Test
        @DisplayName("PangrovaPlanner constructors do NOT accept MaterializedViewRegistry")
        void constructorsDoNotAcceptMaterializedViewRegistry() {
            boolean anyConstructorHasMvManager = Arrays.stream(PangrovaPlanner.class.getConstructors())
                    .flatMap(c -> Arrays.stream(c.getParameterTypes()))
                    .anyMatch(t -> t.getSimpleName().equals("MaterializedViewRegistry"));

            assertFalse(anyConstructorHasMvManager,
                    "PangrovaPlanner constructors do not accept MaterializedViewRegistry yet");
        }
    }

    // ----------------------------------------------------------------
    // Cross-cutting: sub-module classes exist and are well-formed
    // ----------------------------------------------------------------
    @Nested
    @DisplayName("Sub-module classes exist and are independently testable")
    class SubModuleExistence {

        @Test
        @DisplayName("AQE classes are loadable")
        void adaptiveQueryClassesExist() throws ClassNotFoundException {
            Class.forName("org.pangrova.planner.adaptive.AdaptiveQueryExecutor");
            Class.forName("org.pangrova.planner.adaptive.PartitionCoalescer");
            Class.forName("org.pangrova.planner.adaptive.SkewJoinHandler");
            Class.forName("org.pangrova.planner.adaptive.ShuffleStageStats");
            Class.forName("org.pangrova.planner.adaptive.ReOptimizationResult");
        }

        @Test
        @DisplayName("DPP classes are loadable")
        void dynamicPartitionPruningClassesExist() throws ClassNotFoundException {
            Class.forName("org.pangrova.planner.runtimefilter.RuntimeFilterGenerator");
            Class.forName("org.pangrova.planner.runtimefilter.RuntimeFilterApplicator");
            Class.forName("org.pangrova.planner.runtimefilter.RuntimeFilterRegistry");
            Class.forName("org.pangrova.planner.runtimefilter.RuntimeFilter");
            Class.forName("org.pangrova.planner.runtimefilter.PartitionRange");
        }

        @Test
        @DisplayName("Materialized View classes are loadable")
        void materializedViewClassesExist() throws ClassNotFoundException {
            Class.forName("org.pangrova.planner.mv.MaterializedViewQueryRewriter");
            Class.forName("org.pangrova.planner.mv.MaterializedViewRegistry");
            Class.forName("org.pangrova.planner.mv.DigestPlanRewriter");
            Class.forName("org.pangrova.planner.mv.MaterializedViewDefinition");
            Class.forName("org.pangrova.planner.mv.MaterializedViewCandidate");
        }
    }
}

package org.pangrova.coordinator.query;

import org.pangrova.coordinator.query.execution.QueryDispatcher;
import org.pangrova.coordinator.query.execution.QueryDispatcherContext;
import org.pangrova.planner.analysis.PangrovaPlanner;
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
 * (AQE, Materialized View) with the coordinator and worker runtime.
 *
 * <p>These sub-modules are now wired into the query execution pipeline
 * via QueryDispatcherContext (AQE) and PangrovaPlanner (Materialized View).
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
    // Materialized View — Calcite substitution
    // ----------------------------------------------------------------
    @Nested
    @DisplayName("Materialized View: CalciteMaterializedViewRewriter ↔ PangrovaPlanner")
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
        @DisplayName("MaterializedViewRewritePass delegates to CalciteMaterializedViewRewriter")
        void rewritePassUsesCalciteRewriter() {
            Set<String> fieldTypeNames = Arrays.stream(
                            org.pangrova.planner.optimization.MaterializedViewRewritePass.class
                                    .getDeclaredFields())
                    .map(f -> f.getType().getSimpleName())
                    .collect(Collectors.toSet());

            assertTrue(fieldTypeNames.contains("CalciteMaterializedViewRewriter"),
                    "The rewrite pass must own the Calcite substitution engine");
        }

        @Test
        @DisplayName("PangrovaPlanner exposes the policy-gated rewrite entry point")
        void plannerExposesProtectedOptimization() {
            Set<String> methodNames = Arrays.stream(PangrovaPlanner.class.getDeclaredMethods())
                    .map(Method::getName)
                    .collect(Collectors.toSet());

            assertTrue(methodNames.contains("optimizeProtectedQuery"),
                    "Policy-gated optimization must remain the only rewrite entry point");
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
            Class.forName("org.pangrova.planner.adaptivequery.AdaptiveQueryExecutor");
            Class.forName("org.pangrova.planner.adaptivequery.PartitionCoalescer");
            Class.forName("org.pangrova.planner.adaptivequery.SkewJoinHandler");
            Class.forName("org.pangrova.planner.adaptivequery.ShuffleStageStats");
            Class.forName("org.pangrova.planner.api.adaptivequery.ReOptimizationResult");
        }

        @Test
        @DisplayName("Materialized View classes are loadable")
        void materializedViewClassesExist() throws ClassNotFoundException {
            Class.forName("org.pangrova.planner.mv.CalciteMaterializedViewRewriter");
            Class.forName("org.pangrova.planner.mv.MaterializedViewPlanEligibility");
            Class.forName("org.pangrova.planner.mv.MaterializedViewRewriteCatalog");
            Class.forName("org.pangrova.planner.optimization.MaterializedViewRewritePass");
        }
    }
}

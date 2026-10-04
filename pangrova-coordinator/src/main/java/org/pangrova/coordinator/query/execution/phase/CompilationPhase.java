/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.pangrova.coordinator.query.execution.phase;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.pangrova.coordinator.query.session.CoordinatorSessionContext;
import org.pangrova.coordinator.query.audit.QueryLineageCapture;
import org.pangrova.planner.PlannedQuery;
import org.pangrova.planner.PlanResult;
import org.pangrova.planner.SqlCompiler;

import static org.pangrova.common.util.Preconditions.checkNotNull;

/**
 * Phase 2: Captures compiler snapshot, extracts time-travel clauses,
 * compiles SQL through security policy applier, and captures lineage.
 * Produces a compiled plan in the context for downstream phases.
 *
 * <p>Preserves pre-optimization column names so that downstream phases
 * (Substrait conversion) can use original SQL aliases instead of
 * synthetic names produced by HEP rules like PROJECT_TO_LOGICAL_PROJECT_AND_WINDOW.</p>
 */
public final class CompilationPhase implements QueryExecutionPhase {

    /** Applies security policies (access control, column masking, row filtering) to a compiled plan. */
    @FunctionalInterface
    public interface SecurityPolicyEnforcer {
        PlannedQuery applyPolicies(PlannedQuery plan, CoordinatorSessionContext session);
    }

    /** Extracts time-travel specs from SQL, returning stripped SQL and snapshot overrides. */
    @FunctionalInterface
    public interface TimeTravelExtractor {
        TimeTravelResult extract(String sql, SqlCompiler compiler, CoordinatorSessionContext session);
    }

    /** Result of time-travel extraction. */
    public record TimeTravelResult(String strippedSql, Map<String, Long> snapshotOverrides) {
        public static TimeTravelResult passthrough(String sql) {
            return new TimeTravelResult(sql, Map.of());
        }
    }

    private final SecurityPolicyEnforcer securityPolicyEnforcer;
    private final TimeTravelExtractor timeTravelExtractor;
    private final QueryLineageCapture lineageCapture;
    private final Function<SqlCompiler, SqlCompiler> compilerSnapshotFn;

    /** Creates the authenticated bind, policy and optimization phase. */
    public CompilationPhase(
            SecurityPolicyEnforcer securityPolicyEnforcer,
            TimeTravelExtractor timeTravelExtractor,
            QueryLineageCapture lineageCapture,
            Function<SqlCompiler, SqlCompiler> compilerSnapshotFn) {
        this.securityPolicyEnforcer = checkNotNull(securityPolicyEnforcer, "securityPolicyEnforcer");
        this.timeTravelExtractor = checkNotNull(timeTravelExtractor, "timeTravelExtractor");
        this.lineageCapture = checkNotNull(lineageCapture, "lineageCapture");
        this.compilerSnapshotFn = checkNotNull(compilerSnapshotFn, "compilerSnapshotFn");
    }

    @Override
    public PhaseResult execute(QueryExecutionContext context) {
        SqlCompiler snapshot = compilerSnapshotFn.apply(context.compiler());
        var session = org.pangrova.coordinator.query.session.SessionContextConverter.toConnectorSession(context.session());
        SqlCompiler extractionCompiler = snapshot.forSession(session);
        TimeTravelResult ttResult = timeTravelExtractor.extract(context.sql(), extractionCompiler, context.session());
        SqlCompiler currentCompiler = ttResult.snapshotOverrides().isEmpty() ? extractionCompiler
                : snapshot.forSession(org.pangrova.coordinator.metadata.read.PlanningSnapshotSelectors.bind(
                        session, ttResult.snapshotOverrides()));
        QueryExecutionContext updated = context.withCompiler(currentCompiler)
                .withTimeTravelResult(ttResult.strippedSql(), ttResult.snapshotOverrides());

        // Schema, name caches and statistics belong to this authenticated query.
        // Authorize and transform expressions before optimization; policy results must never be cached.
        PlanResult planResult = currentCompiler.compileForPolicyApplication(ttResult.strippedSql());
        List<String> preOptNames = planResult.preOptimizationColumnNames();
        PlannedQuery plannedQuery = planResult.toPlannedQuery();

        // Apply security policies to this authenticated query's plan.
        plannedQuery = securityPolicyEnforcer.applyPolicies(plannedQuery, context.session());
        plannedQuery = currentCompiler.optimizeProtectedQuery(plannedQuery);

        lineageCapture.captureLineageInfo(
                context.session().queryId(), context.originalSql(), plannedQuery);
        lineageCapture.captureColumnNames(
                context.session().queryId(), preOptNames, plannedQuery);
        return new PhaseResult.Continue(
                updated.withCompiledPlan(plannedQuery, preOptNames,
                        plannedQuery.runtimeFilterDescriptors()));
    }

    @Override
    public String name() {
        return "compilation";
    }
}

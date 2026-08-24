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

import org.pangrova.coordinator.query.CoordinatorSessionContext;
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
        TimeTravelResult extract(String sql, SqlCompiler compiler);
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
        SqlCompiler currentCompiler = compilerSnapshotFn.apply(context.compiler());
        TimeTravelResult ttResult = timeTravelExtractor.extract(context.sql(), currentCompiler);
        QueryExecutionContext updated = context
                .withTimeTravelResult(ttResult.strippedSql(), ttResult.snapshotOverrides());

        // PLAN-CACHE TENANT-SAFETY INVARIANT (F-PLAN-CACHE-TENANT):
        // compileToResult() may return a cached optimized plan keyed only by
        // normalized SQL + session-properties digest (PlanCacheKey) — i.e. it is
        // tenant-/principal-/policy-independent. Row-level security and column
        // masking MUST therefore be applied per-session AFTER cache retrieval and
        // MUST NOT be baked into the cached PlanResult; otherwise a plan cached for
        // one tenant could leak another tenant's row filters/masks. Keep
        // applyPolicies() strictly downstream of compileToResult() and never cache
        // the post-policy PlannedQuery.
        PlanResult planResult = currentCompiler.compileToResult(ttResult.strippedSql());
        List<String> preOptNames = planResult.preOptimizationColumnNames();
        PlannedQuery plannedQuery = planResult.toPlannedQuery();

        // Apply per-session security policies to the (possibly cached) tenant-independent plan.
        plannedQuery = securityPolicyEnforcer.applyPolicies(plannedQuery, context.session());

        lineageCapture.captureLineageInfo(
                context.session().queryId(), context.originalSql(), plannedQuery);
        lineageCapture.captureColumnNames(
                context.session().queryId(), preOptNames, plannedQuery);
        return new PhaseResult.Continue(
                updated.withCompiledPlan(plannedQuery, preOptNames,
                        planResult.runtimeFilterDescriptors()));
    }

    @Override
    public String name() {
        return "compilation";
    }
}

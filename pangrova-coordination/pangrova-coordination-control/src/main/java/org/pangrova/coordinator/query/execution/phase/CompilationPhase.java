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

import org.pangrova.connector.api.metadata.ConnectorReadDiscovery;
import org.pangrova.connector.api.metadata.table.QualifiedTableName;
import org.pangrova.query.api.session.QuerySession;
import org.pangrova.coordinator.query.audit.QueryLineageCapture;
import org.pangrova.planner.analysis.PlannedQuery;
import org.pangrova.planner.analysis.PlanResult;
import org.pangrova.planner.analysis.SqlCompiler;
import org.pangrova.parser.parse.TimeTravelSqlClauses.Analysis;

import static org.pangrova.common.validation.Preconditions.checkNotNull;

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
        ProtectedPlan applyPolicies(PlannedQuery plan, QuerySession session);
    }

    /**
     * One protected plan and whether policy expressions were injected.
     *
     * @param plan authorized and policy-protected plan
     * @param policiesApplied true when a row filter or column mask was applied
     */
    public record ProtectedPlan(PlannedQuery plan, boolean policiesApplied) {
        public ProtectedPlan {
            checkNotNull(plan, "plan");
        }
    }

    /** Extracts time-travel specs from SQL, returning stripped SQL and snapshot overrides. */
    @FunctionalInterface
    public interface TimeTravelExtractor {
        TimeTravelResult extract(Analysis input, QuerySession session);
    }

    /** Result of time-travel extraction. */
    public record TimeTravelResult(Analysis parsedSql, Map<String, Long> snapshotOverrides,
            Map<QualifiedTableName, ConnectorReadDiscovery> discoveries) {
        public TimeTravelResult {
            snapshotOverrides = Map.copyOf(snapshotOverrides);
            discoveries = Map.copyOf(discoveries);
        }

        public String strippedSql() {
            return parsedSql.strippedSql();
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
        var session = org.pangrova.coordinator.query.session.SessionContextConverter
                .toConnectorSession(context.session());
        TimeTravelResult timeTravel = context.metadataQuery() == null
                ? timeTravelExtractor.extract(context.parsedSql(), context.session()) : null;
        Map<String, Long> overrides = timeTravel == null ? Map.of() : timeTravel.snapshotOverrides();
        SqlCompiler currentCompiler = snapshot.forSession(overrides.isEmpty() ? session
                : org.pangrova.planner.metadata.catalog.PlanningSnapshotSelectors.bind(
                        session, overrides, timeTravel.discoveries()), context.session().publicationRevision());

        // Schema, name caches and statistics belong to this authenticated query.
        // Authorize and transform expressions before optimization; policy results must never be cached.
        PlanResult planResult = timeTravel == null
                ? currentCompiler.compileForPolicyApplication(context.metadataQuery())
                : currentCompiler.compileForPolicyApplication(timeTravel.parsedSql());
        List<String> preOptNames = planResult.preOptimizationColumnNames();
        PlannedQuery plannedQuery = planResult.toPlannedQuery();

        // Apply security policies to this authenticated query's plan.
        ProtectedPlan protectedPlan = securityPolicyEnforcer.applyPolicies(
                plannedQuery, context.session());
        plannedQuery = currentCompiler.optimizeProtectedQuery(protectedPlan.plan(),
                rewriteIsProven(protectedPlan, overrides, context.session()));

        context.stateMachine().bindLineageMetadata(lineageCapture.captureLineageInfo(
                context.session().queryId(), context.originalSql(), plannedQuery));
        lineageCapture.captureColumnNames(
                context.session().queryId(), preOptNames, plannedQuery);
        return new PhaseResult.Continue(context.withCompilation(currentCompiler, plannedQuery, preOptNames,
                timeTravel == null ? null : timeTravel.parsedSql(), overrides));
    }

    @Override
    public String name() {
        return "compilation";
    }

    /**
     * A published snapshot may only replace source scans when the query is an
     * unfiltered, unmasked read of the current generation without a snapshot selector.
     */
    private static boolean rewriteIsProven(ProtectedPlan protectedPlan,
            Map<String, Long> overrides, QuerySession session) {
        return !protectedPlan.policiesApplied() && overrides.isEmpty()
                && session.sourceSnapshots().isEmpty() && session.sourceIncrementalRanges().isEmpty();
    }
}

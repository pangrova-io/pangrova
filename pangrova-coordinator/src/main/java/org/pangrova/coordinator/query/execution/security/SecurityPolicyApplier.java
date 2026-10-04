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
package org.pangrova.coordinator.query.execution.security;

import org.pangrova.coordinator.query.session.CoordinatorSessionContext;
import org.pangrova.coordinator.query.security.AccessControlEnforcer;
import org.pangrova.planner.PlannedQuery;
import org.pangrova.planner.SqlCompiler;
import org.pangrova.planner.security.ColumnMaskApplier;
import org.pangrova.planner.security.RowFilterApplier;

/**
 * Compiles SQL to a planned query and applies security policies (access control,
 * column masking, row-level filtering) in the correct order.
 * Extracted from QueryDispatcher to reduce God Class complexity.
 */
public class SecurityPolicyApplier {

    private final VersionedCompilerHolder compilerHolder;
    private final AccessControlEnforcer accessControlEnforcer;
    private final ColumnMaskApplier columnMaskApplier;
    private final RowFilterApplier rowFilterApplier;

    /** Creates the query-local compilation and policy assembly. */
    public SecurityPolicyApplier(SqlCompiler compiler,
                                 AccessControlEnforcer accessControlEnforcer,
                                 ColumnMaskApplier columnMaskApplier,
                                 RowFilterApplier rowFilterApplier) {
        this.compilerHolder = new VersionedCompilerHolder(compiler);
        this.accessControlEnforcer = accessControlEnforcer;
        this.columnMaskApplier = columnMaskApplier;
        this.rowFilterApplier = rowFilterApplier;
    }

    /**
     * Atomically updates the compiler after schema changes (DDL).
     * Version is auto-incremented.
     */
    public void updateCompiler(SqlCompiler newCompiler) {
        compilerHolder.update(newCompiler);
    }

    /**
     * Returns the current compiler for use by other components.
     */
    public SqlCompiler getCompiler() {
        return compilerHolder.compiler();
    }

    /**
     * Returns the versioned compiler holder for callers that need
     * to pin a consistent compiler snapshot across a query lifecycle.
     */
    public VersionedCompilerHolder compilerHolder() {
        return compilerHolder;
    }

    /**
     * Compiles SQL to a PlannedQuery and applies all security policies in order:
     * 1. Access control check (table-level)
     * 2. Column masking
     * 3. Row-level filtering
     */
    public PlannedQuery compileAndApplyPolicies(String sql, CoordinatorSessionContext session) {
        return compileAndApplyPolicies(sql, session, compilerHolder.compiler());
    }

    /**
     * Applies security policies to a pre-compiled PlannedQuery.
     * Used by CompilationPhase which compiles via SqlCompiler.compileToResult()
     * to preserve pre-optimization column names.
     *
     * <p>Policy application order:
     * 1. Access control check (table-level)
     * 2. Column masking
     * 3. Row-level filtering</p>
     */
    public PlannedQuery applyPolicies(PlannedQuery plannedQuery,
                                      CoordinatorSessionContext session) {
        return applyPoliciesObserved(plannedQuery, session).plan();
    }

    /** Applies the same authority while retaining the exact policy values consumed. */
    public ObservedPlan applyPoliciesObserved(PlannedQuery plan, CoordinatorSessionContext session) {
        if (accessControlEnforcer == null || columnMaskApplier == null || rowFilterApplier == null) {
            throw new org.pangrova.spi.security.AccessDeniedException(
                    "Protected compilation requires complete authorization and policy enforcement");
        }
        accessControlEnforcer.checkTableAccess(plan.tableReferences(), session.userIdentity());
        ObservedQueryPolicies observations = new ObservedQueryPolicies();
        PlannedQuery protectedPlan = plan
                .applyColumnMasks(columnMaskApplier.observing(observations), session.userIdentity())
                .applyRowFilters(rowFilterApplier.observing(observations), session.userIdentity());
        return new ObservedPlan(protectedPlan, observations.snapshot());
    }

    /** One protected plan and observations from exactly that policy application. */
    public record ObservedPlan(PlannedQuery plan,
            java.util.Map<String, org.pangrova.security.authz.DeferredAuthorizationScope.PolicySnapshot> policies) {
        public ObservedPlan {
            policies = java.util.Map.copyOf(policies);
        }
    }

    /**
     * Compiles SQL to a PlannedQuery using the provided compiler snapshot and applies
     * all security policies. Use this overload when a consistent compiler reference
     * must be shared across compile and serialize phases.
     */
    public PlannedQuery compileAndApplyPolicies(String sql, CoordinatorSessionContext session,
                                                SqlCompiler compilerSnapshot) {
        SqlCompiler compiler = compilerSnapshot.forSession(
                org.pangrova.coordinator.query.session.SessionContextConverter.toConnectorSession(session));
        PlannedQuery query = compiler.compileForPolicyApplication(sql).toPlannedQuery();
        return compiler.optimizeProtectedQuery(applyPoliciesObserved(query, session).plan());
    }
}

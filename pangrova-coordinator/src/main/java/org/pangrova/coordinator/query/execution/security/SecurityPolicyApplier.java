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

import org.pangrova.coordinator.query.CoordinatorSessionContext;
import org.pangrova.coordinator.query.security.AccessControlEnforcer;
import org.pangrova.planner.PlannedQuery;
import org.pangrova.planner.SqlCompiler;
import org.pangrova.planner.security.ColumnMaskApplier;
import org.pangrova.planner.security.RowFilterApplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Compiles SQL to a planned query and applies security policies (access control,
 * column masking, row-level filtering) in the correct order.
 * Extracted from QueryDispatcher to reduce God Class complexity.
 */
public class SecurityPolicyApplier {

    private static final Logger LOG = LoggerFactory.getLogger(SecurityPolicyApplier.class);

    private final VersionedCompilerHolder compilerHolder;
    private final AccessControlEnforcer accessControlEnforcer;
    private final ColumnMaskApplier columnMaskApplier;
    private final RowFilterApplier rowFilterApplier;

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
        if (accessControlEnforcer != null) {
            accessControlEnforcer.checkTableAccess(
                    plannedQuery.tableReferences(), session.userIdentity());
        }
        if (columnMaskApplier != null) {
            plannedQuery = plannedQuery.applyColumnMasks(columnMaskApplier, session.userIdentity());
        }
        if (rowFilterApplier != null) {
            plannedQuery = plannedQuery.applyRowFilters(rowFilterApplier, session.userIdentity());
        }
        return plannedQuery;
    }

    /**
     * Compiles SQL to a PlannedQuery using the provided compiler snapshot and applies
     * all security policies. Use this overload when a consistent compiler reference
     * must be shared across compile and serialize phases.
     */
    public PlannedQuery compileAndApplyPolicies(String sql, CoordinatorSessionContext session,
                                                SqlCompiler compilerSnapshot) {
        PlannedQuery plannedQuery = compileSql(sql, compilerSnapshot);
        return applyPolicies(plannedQuery, session);
    }

    private PlannedQuery compileSql(String sql, SqlCompiler compilerToUse) {
        try {
            return compilerToUse.compileToPlannedQuery(sql);
        } catch (Exception e) {
            if (isReduceExpressionsCrash(e)) {
                LOG.warn("event=compile_reduce_expressions_crash action=retry_safe sql_length={}",
                        sql.length());
                return compilerToUse.planner().planSafe(sql).toPlannedQuery();
            }
            throw e;
        }
    }

    private static boolean isReduceExpressionsCrash(Exception e) {
        String msg = e.getMessage();
        if (msg == null) {
            msg = "";
        }
        return msg.contains("-2147483648")
                || (e instanceof IndexOutOfBoundsException && msg.contains("out of bounds"));
    }
}

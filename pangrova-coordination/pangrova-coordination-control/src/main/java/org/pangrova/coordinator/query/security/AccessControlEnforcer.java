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
package org.pangrova.coordinator.query.security;

import org.pangrova.planner.enums.PlannedDmlOperation;
import org.pangrova.planner.analysis.DmlWriteTarget;
import org.pangrova.planner.security.ColumnMaskApplier;
import org.pangrova.planner.security.RowFilterApplier;
import org.pangrova.connector.api.metadata.table.QualifiedTableName;
import org.pangrova.security.api.authorization.AccessControl;
import org.pangrova.security.api.authorization.TableReadPolicySource;
import org.pangrova.security.api.exception.AccessDeniedException;
import org.pangrova.security.api.identity.UserIdentity;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

import static org.pangrova.common.util.Preconditions.checkNotNull;

/**
 * Enforces access control on compiled plans for all operation types:
 * SELECT (via table references), INSERT/UPDATE/DELETE/MERGE (via DML write target),
 * and DDL operations (via explicit statement-level checks).
 *
 * <p>Thread-safe: stateless beyond the injected {@code AccessControl} reference.</p>
 */
public final class AccessControlEnforcer {

    private final AccessControl accessControl;

    public AccessControlEnforcer(AccessControl accessControl) {
        this.accessControl = checkNotNull(accessControl, "accessControl must not be null");
    }

    /**
     * Checks access control for every table referenced in the plan.
     * Enforces SELECT privilege on all scan references and write privilege
     * on the DML target (if present).
     *
     * @param tableReferences qualified table names from all TableScan nodes
     * @param dmlWriteTargets the DML write targets if this is a DML statement, empty otherwise
     * @param identity        the authenticated user
     * @throws AccessDeniedException if identity is null or any table access is denied
     */
    public void checkTableAccess(List<List<String>> tableReferences,
                                  List<DmlWriteTarget> dmlWriteTargets,
                                  UserIdentity identity) {
        checkTableAccess(tableReferences, dmlWriteTargets, identity, Map.of());
    }

    /** Checks physical scans against their query-owned logical Dynamic Table identity. */
    public void checkTableAccess(List<List<String>> tableReferences,
                                  List<DmlWriteTarget> dmlWriteTargets,
                                  UserIdentity identity,
                                  Map<QualifiedTableName, QualifiedTableName> physicalToLogical) {
        checkTableAccess(tableReferences, dmlWriteTargets, identity, physicalToLogical, false);
    }

    /**
     * Checks table access for a plan, optionally admitting kernel-authored
     * materialization execution.
     *
     * <p>Internal execution may read its pinned base generation and write the
     * fenced candidate under the reserved namespace; every other reference,
     * and every user-issued statement, keeps the reserved-name guard.</p>
     */
    public void checkTableAccess(List<List<String>> tableReferences,
                                  List<DmlWriteTarget> dmlWriteTargets,
                                  UserIdentity identity,
                                  Map<QualifiedTableName, QualifiedTableName> physicalToLogical,
                                  boolean internalMaterializationExecution) {
        authorizeReadTables(tableReferences, identity, physicalToLogical, internalMaterializationExecution);
        for (DmlWriteTarget target : dmlWriteTargets) {
            QualifiedTableName ref = QualifiedTableName.resolve(target.qualifiedTableName());
            if (!internalMaterializationExecution || !isReservedDynamicTableGeneration(ref)) {
                rejectReservedGeneration(ref);
            }
            enforceWritePrivilege(identity, ref, target.operation());
        }
    }

    /** Requires all protected-plan components to use the same authorization provider. */
    public void requireSharedPolicyAuthority(ColumnMaskApplier masks, RowFilterApplier filters) {
        if (!masks.usesAuthority(accessControl) || !filters.usesAuthority(accessControl)) {
            throw new IllegalArgumentException("Query admission and row/column policies require one authority");
        }
    }

    /** Authorizes the scans and returns the same subject context for their policy evaluation. */
    public AccessControl.ReadAuthorization authorizeReadTables(List<List<String>> tableReferences,
            UserIdentity identity, Map<QualifiedTableName, QualifiedTableName> physicalToLogical,
            boolean internalMaterializationExecution) {
        rejectAnonymous(identity);
        var authorization = accessControl.beginReadAuthorization(identity);
        if (tableReferences.isEmpty()) return authorization;
        var policyTargets = tableReferences.size() > 1 ? new LinkedHashSet<TableReadPolicySource.Target>() : null;
        for (List<String> qualifiedName : tableReferences) {
            QualifiedTableName ref = QualifiedTableName.resolve(qualifiedName);
            QualifiedTableName logical = physicalToLogical.get(ref);
            if (!internalMaterializationExecution
                    && isReservedDynamicTableGeneration(ref) && logical == null) {
                throw new AccessDeniedException("Direct access to a Dynamic Table generation is forbidden");
            }
            QualifiedTableName authorized = logical == null ? ref : logical;
            String table = authorizedReadTable(authorized.table());
            if (table.equals(authorized.table())) {
                authorization.requireSelect(authorized.catalog(), authorized.schema(), table);
                if (policyTargets != null) {
                    policyTargets.add(new TableReadPolicySource.Target(
                            authorized.catalog(), authorized.schema(), table));
                }
            } else {
                authorization.requireUnfilteredMetadata(authorized.catalog(), authorized.schema(), table);
            }
        }
        if (policyTargets != null) authorization.preparePolicies(policyTargets);
        return authorization;
    }

    /**
     * Checks column-level SELECT privileges for projected columns.
     * F-SEC-03: Enforces column-scoped grants cannot widen to full-table read.
     *
     * @param catalog  the catalog name
     * @param schema   the schema name
     * @param table    the table name
     * @param columns  the projected column names
     * @param identity the authenticated user
     * @throws AccessDeniedException if identity lacks SELECT on any referenced column
     */
    public void checkColumnAccess(String catalog, String schema, String table,
                                   java.util.Set<String> columns, UserIdentity identity) {
        rejectAnonymous(identity);
        rejectReservedGeneration(new QualifiedTableName(catalog, schema, table));
        String baseTable = authorizedReadTable(table);
        if (!baseTable.equals(table)) {
            accessControl.checkCanReadTableMetadata(identity, catalog, schema, baseTable);
            return;
        }
        if (columns != null && !columns.isEmpty()) {
            accessControl.checkCanSelectFromColumns(identity, catalog, schema, table, columns);
        }
    }

    /**
     * Checks access control using only table references (no DML write targets).
     * Convenience overload for SELECT-only plans.
     */
    public void checkTableAccess(List<List<String>> tableReferences, UserIdentity identity) {
        checkTableAccess(tableReferences, List.of(), identity);
    }

    /** A read-only mapped scan must have come from the durable planner rewrite. */
    public void checkTableAccess(List<List<String>> tableReferences, UserIdentity identity,
                                  Map<QualifiedTableName, QualifiedTableName> physicalToLogical) {
        checkTableAccess(tableReferences, List.of(), identity, physicalToLogical);
    }

    /** Read-only variant for kernel-authored materialization execution. */
    public void checkTableAccess(List<List<String>> tableReferences, UserIdentity identity,
                                  Map<QualifiedTableName, QualifiedTableName> physicalToLogical,
                                  boolean internalMaterializationExecution) {
        checkTableAccess(tableReferences, List.of(), identity, physicalToLogical,
                internalMaterializationExecution);
    }

    /** Reserved physical names cannot be directly selected or created by users. */
    public static boolean isReservedDynamicTableGeneration(String table) {
        return org.pangrova.materialization.api.domain.InternalNamespace.isReservedTable(table);
    }

    /** The internal schema and every generation table in it are inaccessible without a rewrite pin. */
    public static boolean isReservedDynamicTableGeneration(QualifiedTableName table) {
        return table != null
                && (org.pangrova.materialization.api.domain.InternalNamespace.isReservedSchema(table.schema())
                        || isReservedDynamicTableGeneration(table.table()));
    }

    private void enforceWritePrivilege(UserIdentity identity, QualifiedTableName ref,
                                        PlannedDmlOperation operation) {
        String catalog = ref.catalog();
        String schema = ref.schema();
        String table = ref.table();
        switch (operation) {
            case INSERT -> accessControl.checkCanInsertIntoTable(identity, catalog, schema, table);
            case DELETE -> accessControl.checkCanDeleteFromTable(identity, catalog, schema, table);
            case UPDATE -> accessControl.checkCanUpdateTable(identity, catalog, schema, table);
            case MERGE -> throw new IllegalStateException("MERGE authority requires original clause actions");
            default -> throw new IllegalStateException("Unhandled write operation: " + operation);
        }
    }

    /**
     * Checks DDL privilege for CREATE TABLE operations.
     *
     * @throws AccessDeniedException if identity is null or privilege is denied
     */
    public void checkCreateTable(UserIdentity identity, String catalog, String schema, String table) {
        rejectAnonymous(identity);
        accessControl.checkCanCreateTable(identity, catalog, schema, table);
    }

    /**
     * Checks DDL privilege for ALTER TABLE operations.
     *
     * @throws AccessDeniedException if identity is null or privilege is denied
     */
    public void checkAlterTable(UserIdentity identity, String catalog, String schema, String table) {
        rejectAnonymous(identity);
        accessControl.checkCanAlterTable(identity, catalog, schema, table);
    }

    /**
     * Checks DDL privilege for DROP TABLE operations.
     *
     * @throws AccessDeniedException if identity is null or privilege is denied
     */
    public void checkDropTable(UserIdentity identity, String catalog, String schema, String table) {
        rejectAnonymous(identity);
        accessControl.checkCanDropTable(identity, catalog, schema, table);
    }

    /**
     * Checks DML privilege for INSERT operations (statement-level, before plan compilation).
     *
     * @throws AccessDeniedException if identity is null or privilege is denied
     */
    public void checkInsertIntoTable(UserIdentity identity, String catalog, String schema, String table) {
        rejectAnonymous(identity);
        accessControl.checkCanInsertIntoTable(identity, catalog, schema, table);
    }

    /**
     * Checks DML privilege for DELETE operations (statement-level, before plan compilation).
     *
     * @throws AccessDeniedException if identity is null or privilege is denied
     */
    public void checkDeleteFromTable(UserIdentity identity, String catalog, String schema, String table) {
        rejectAnonymous(identity);
        accessControl.checkCanDeleteFromTable(identity, catalog, schema, table);
    }

    /**
     * Checks the original SQL UPDATE privilege, not physical overwrite implementation details.
     *
     * @throws AccessDeniedException if identity is null or privilege is denied
     */
    public void checkUpdateTable(UserIdentity identity, String catalog, String schema, String table) {
        rejectAnonymous(identity);
        accessControl.checkCanUpdateTable(identity, catalog, schema, table);
    }

    /**
     * Checks SELECT privilege on a single table (statement-level, before data access).
     *
     * @throws AccessDeniedException if identity is null or SELECT privilege is denied
     */
    public void checkSelectFromTable(UserIdentity identity, String catalog, String schema, String table) {
        rejectAnonymous(identity);
        rejectReservedGeneration(new QualifiedTableName(catalog, schema, table));
        checkRead(accessControl.beginReadAuthorization(identity), catalog, schema, table);
    }

    private static void checkRead(AccessControl.ReadAuthorization authorization,
            String catalog, String schema, String table) {
        String baseTable = authorizedReadTable(table);
        if (baseTable.equals(table)) {
            authorization.requireSelect(catalog, schema, table);
        } else {
            authorization.requireUnfilteredMetadata(catalog, schema, baseTable);
        }
    }

    private static void rejectReservedGeneration(QualifiedTableName table) {
        if (isReservedDynamicTableGeneration(table)) {
            throw new AccessDeniedException("Direct access to a Dynamic Table generation is forbidden");
        }
    }

    /** Metadata-table contents belong to the base table, never a separate grant. */
    private static String authorizedReadTable(String table) {
        return org.pangrova.connector.api.enums.MetadataTableKind.baseTableName(table);
    }

    private void rejectAnonymous(UserIdentity identity) {
        if (identity == null) {
            throw new AccessDeniedException("Anonymous access is not allowed");
        }
    }
}

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

import org.pangrova.planner.DmlWriteTarget;
import org.pangrova.spi.connector.metadata.table.QualifiedTableName;
import org.pangrova.spi.security.AccessControl;
import org.pangrova.spi.security.AccessDeniedException;
import org.pangrova.spi.security.UserIdentity;

import java.util.List;

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
        rejectAnonymous(identity);
        for (List<String> qualifiedName : tableReferences) {
            QualifiedTableName ref = QualifiedTableName.resolve(qualifiedName);
            accessControl.checkCanSelectFromTable(
                    identity, ref.catalog(), ref.schema(), ref.table());
        }
        for (DmlWriteTarget target : dmlWriteTargets) {
            QualifiedTableName ref = QualifiedTableName.resolve(target.qualifiedTableName());
            enforceWritePrivilege(identity, ref, target.operation());
        }
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

    private void enforceWritePrivilege(UserIdentity identity, QualifiedTableName ref,
                                        DmlWriteTarget.DmlOperation operation) {
        String catalog = ref.catalog();
        String schema = ref.schema();
        String table = ref.table();
        switch (operation) {
            case INSERT -> accessControl.checkCanInsertIntoTable(identity, catalog, schema, table);
            case DELETE -> accessControl.checkCanDeleteFromTable(identity, catalog, schema, table);
            case UPDATE, MERGE -> {
                accessControl.checkCanInsertIntoTable(identity, catalog, schema, table);
                accessControl.checkCanDeleteFromTable(identity, catalog, schema, table);
            }
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
     * Checks DML privilege for UPDATE/MERGE operations which require both INSERT and DELETE.
     *
     * @throws AccessDeniedException if identity is null or privilege is denied
     */
    public void checkUpdateTable(UserIdentity identity, String catalog, String schema, String table) {
        rejectAnonymous(identity);
        accessControl.checkCanInsertIntoTable(identity, catalog, schema, table);
        accessControl.checkCanDeleteFromTable(identity, catalog, schema, table);
    }

    /**
     * Checks SELECT privilege on a single table (statement-level, before data access).
     *
     * @throws AccessDeniedException if identity is null or SELECT privilege is denied
     */
    public void checkSelectFromTable(UserIdentity identity, String catalog, String schema, String table) {
        rejectAnonymous(identity);
        accessControl.checkCanSelectFromTable(identity, catalog, schema, table);
    }

    private void rejectAnonymous(UserIdentity identity) {
        if (identity == null) {
            throw new AccessDeniedException("Anonymous access is not allowed");
        }
    }
}

package org.pangrova.coordinator.query.execution.dispatch;

import org.pangrova.coordinator.query.statement.StatementClassifier;
import org.pangrova.parser.ast.Statement;
import org.pangrova.parser.ast.statement.DropTableStatement;
import org.pangrova.parser.ast.statement.dml.DeleteStatement;
import org.pangrova.parser.ast.statement.dml.MergeStatement;
import org.pangrova.parser.ast.statement.dml.UpdateStatement;
import org.pangrova.spi.connector.metadata.table.QualifiedTableName;
import org.pangrova.spi.security.AccessControl;
import org.pangrova.spi.security.UserIdentity;

/**
 * Stateless utility that enforces table-level access control for parsed statements.
 * Consolidates permission checking into a single location, replacing duplicate
 * {@code checkPermission()} implementations across dispatch layers.
 */
public final class PermissionChecker {

    private PermissionChecker() {}

    /**
     * Enforces table-level access control for the given statement.
     * Skips silently if accessControl or user is null.
     */
    public static void check(Statement stmt, UserIdentity user,
                              AccessControl accessControl) {
        if (accessControl == null || user == null) return;
        QualifiedTableName parts = StatementClassifier.extractTableParts(stmt);
        if (parts == null) return;
        String cat = parts.catalog();
        String sch = parts.schema();
        String tbl = parts.table();

        switch (StatementCategory.categorize(stmt)) {
            case DML -> checkDml(stmt, user, accessControl, cat, sch, tbl);
            case DDL -> checkDdl(stmt, user, accessControl, cat, sch, tbl);
            case INDEX, CLUSTERING, DYNAMIC_TABLE ->
                    accessControl.checkCanAlterTable(user, cat, sch, tbl);
            default -> { /* SHOW, EXPLAIN, MAINTENANCE, TENANT, STREAM: no table-level check */ }
        }
    }

    private static void checkDml(Statement stmt, UserIdentity user,
                                  AccessControl ac, String c, String s, String t) {
        if (stmt instanceof DeleteStatement) {
            ac.checkCanDeleteFromTable(user, c, s, t);
        } else if (stmt instanceof UpdateStatement || stmt instanceof MergeStatement) {
            ac.checkCanInsertIntoTable(user, c, s, t);
            ac.checkCanDeleteFromTable(user, c, s, t);
        } else {
            ac.checkCanInsertIntoTable(user, c, s, t);
        }
    }

    private static void checkDdl(Statement stmt, UserIdentity user,
                                  AccessControl ac, String c, String s, String t) {
        if (stmt instanceof DropTableStatement) {
            ac.checkCanDropTable(user, c, s, t);
        } else {
            ac.checkCanAlterTable(user, c, s, t);
        }
    }
}

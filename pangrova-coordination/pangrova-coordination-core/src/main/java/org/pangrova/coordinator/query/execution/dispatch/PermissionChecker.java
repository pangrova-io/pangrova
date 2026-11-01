package org.pangrova.coordinator.query.execution.dispatch;

import org.pangrova.coordinator.query.statement.StatementClassifier;
import org.pangrova.coordinator.query.statement.TableResolver;
import org.pangrova.coordinator.query.security.AccessControlEnforcer;
import org.pangrova.coordinator.query.session.CoordinatorSessionContext;
import org.pangrova.parser.ast.CreateCatalogStatement;
import org.pangrova.parser.ast.CreateDynamicTableStatement;
import org.pangrova.parser.ast.CreateMaterializedViewStatement;
import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.Statement;
import org.pangrova.parser.ast.statement.AlterStageStatement;
import org.pangrova.parser.ast.statement.AlterVolumeStatement;
import org.pangrova.parser.ast.statement.CreateStageStatement;
import org.pangrova.parser.ast.statement.CreateVolumeStatement;
import org.pangrova.parser.ast.statement.DropStageStatement;
import org.pangrova.parser.ast.statement.DropVolumeStatement;
import org.pangrova.parser.ast.statement.DropDynamicTableStatement;
import org.pangrova.parser.ast.statement.DropMaterializedViewStatement;
import org.pangrova.parser.ast.statement.RefreshMaterializedViewStatement;
import org.pangrova.parser.ast.statement.AlterTableStatement;
import org.pangrova.parser.ast.statement.CreateSchemaStatement;
import org.pangrova.parser.ast.statement.CreateTableStatement;
import org.pangrova.parser.ast.statement.CreateTableAsSelectStatement;
import org.pangrova.parser.ast.statement.DropCatalogStatement;
import org.pangrova.parser.ast.statement.CreateStorageProfileStatement;
import org.pangrova.parser.ast.statement.DropStorageProfileStatement;
import org.pangrova.parser.ast.statement.DropTableStatement;
import org.pangrova.parser.ast.statement.dml.DeleteStatement;
import org.pangrova.parser.ast.statement.dml.MergeStatement;
import org.pangrova.parser.ast.statement.dml.UpdateStatement;
import org.pangrova.spi.connector.metadata.table.QualifiedTableName;
import org.pangrova.spi.security.AccessControl;
import org.pangrova.spi.security.AccessDeniedException;
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
     * Storage profile mutations require an authenticated actor and access-control
     * provider. Other checks retain their existing nullable bootstrap behavior.
     */
    public static void check(Statement stmt, CoordinatorSessionContext context,
                              AccessControl accessControl) {
        UserIdentity user = context == null ? null : context.userIdentity();
        if (stmt instanceof CreateStorageProfileStatement || stmt instanceof DropStorageProfileStatement) {
            if (accessControl == null || user == null) {
                throw new AccessDeniedException("Authenticated storage profile administrator is required");
            }
            String profileName = stmt instanceof CreateStorageProfileStatement create
                    ? create.profileName() : ((DropStorageProfileStatement) stmt).profileName();
            accessControl.checkCanManageStorageProfile(user, profileName);
            return;
        }
        if (StatementClassifier.isFileSpaceDdl(stmt)) {
            if (accessControl == null || user == null) {
                throw new AccessDeniedException("Authenticated volume or stage principal is required");
            }
            checkFileSpaceMutation(stmt, context, accessControl, user);
            return;
        }
        QualifiedTableName parts = context == null ? null
                : StatementClassifier.extractTableParts(stmt, context);
        checkReservedDynamicTableGeneration(stmt, parts);
        if (accessControl == null || user == null) return;
        if (checkCatalogOrSchema(stmt, context, accessControl, user)) return;
        if (parts == null) return;
        switch (StatementCategory.categorize(stmt)) {
            case DML -> checkDml(stmt, user, accessControl, parts.catalog(), parts.schema(), parts.table());
            case DDL -> checkDdl(stmt, user, accessControl, parts);
            case DYNAMIC_TABLE -> checkDynamicTable(stmt, user, accessControl, parts);
            case MATERIALIZED_VIEW -> checkMaterializedView(stmt, user, accessControl, parts);
            case INDEX, CLUSTERING ->
                    accessControl.checkCanAlterTable(user, parts.catalog(), parts.schema(), parts.table());
            case MAINTENANCE -> checkMaintenance(stmt, user, accessControl, parts);
            default -> { /* Non-table operations have their own authority boundary. */ }
        }
    }

    private static void checkMaintenance(Statement stmt, UserIdentity user,
            AccessControl accessControl, QualifiedTableName parts) {
        boolean readOnly = stmt instanceof org.pangrova.parser.ast.statement.maintenance.AnalyzeStatement
                || (stmt instanceof org.pangrova.parser.ast.statement.maintenance.CallStatement call
                && org.pangrova.coordinator.query.statement.TableWarmupExecutor.PROCEDURE_NAME
                .equalsIgnoreCase(call.procedureName().parts().get(call.procedureName().parts().size() - 1)));
        if (readOnly) {
            accessControl.checkCanSelectFromTable(user, parts.catalog(), parts.schema(), parts.table());
        } else {
            accessControl.checkCanAlterTable(user, parts.catalog(), parts.schema(), parts.table());
        }
    }

    private static void checkFileSpaceMutation(Statement stmt, CoordinatorSessionContext context,
            AccessControl accessControl, UserIdentity user) {
        FileSpaceTarget target = fileSpaceTarget(stmt, context);
        String catalog = target.catalog();
        String schema = target.schema();
        String name = target.name();
        if (stmt instanceof CreateVolumeStatement create) {
            accessControl.checkCanCreateVolume(user, catalog, schema, name, create.external());
        } else if (stmt instanceof CreateStageStatement) {
            accessControl.checkCanCreateStage(user, catalog, schema, name);
        } else if (stmt instanceof AlterVolumeStatement || stmt instanceof DropVolumeStatement) {
            accessControl.checkCanManageVolume(user, catalog, schema, name);
        } else {
            accessControl.checkCanManageStage(user, catalog, schema, name);
        }
    }

    private static FileSpaceTarget fileSpaceTarget(Statement stmt,
            CoordinatorSessionContext context) {
        QualifiedTableName ref = TableResolver.resolveTableRef(fileSpaceName(stmt).parts(), context);
        return new FileSpaceTarget(ref.catalog(), ref.schema(), ref.table());
    }

    private static Identifier fileSpaceName(Statement stmt) {
        if (stmt instanceof CreateVolumeStatement create) {
            return create.name();
        }
        if (stmt instanceof AlterVolumeStatement alter) {
            return alter.name();
        }
        if (stmt instanceof DropVolumeStatement drop) {
            return drop.name();
        }
        if (stmt instanceof CreateStageStatement create) {
            return create.name();
        }
        if (stmt instanceof AlterStageStatement alter) {
            return alter.name();
        }
        return ((DropStageStatement) stmt).name();
    }

    private record FileSpaceTarget(String catalog, String schema, String name) {}

    private static void checkReservedDynamicTableGeneration(Statement stmt,
            QualifiedTableName parts) {
        if (parts != null && AccessControlEnforcer.isReservedDynamicTableGeneration(parts)) {
            throw new AccessDeniedException("Dynamic Table physical generation names are reserved");
        }
        if (stmt instanceof AlterTableStatement.RenameTable rename && parts != null) {
            var target = TableResolver.resolveRenameTarget(rename.newName().parts(), parts);
            if (AccessControlEnforcer.isReservedDynamicTableGeneration(target)) {
                throw new AccessDeniedException("Dynamic Table physical generation names are reserved");
            }
        }
    }

    private static void checkDynamicTable(Statement stmt, UserIdentity user,
            AccessControl accessControl, QualifiedTableName table) {
        if (stmt instanceof CreateDynamicTableStatement) {
            accessControl.checkCanCreateTable(user, table.catalog(), table.schema(), table.table());
        } else if (stmt instanceof DropDynamicTableStatement) {
            accessControl.checkCanDropTable(user, table.catalog(), table.schema(), table.table());
        } else {
            accessControl.checkCanAlterTable(user, table.catalog(), table.schema(), table.table());
        }
    }

    private static void checkMaterializedView(Statement stmt, UserIdentity user,
            AccessControl accessControl, QualifiedTableName table) {
        if (stmt instanceof CreateMaterializedViewStatement) {
            accessControl.checkCanCreateTable(user, table.catalog(), table.schema(), table.table());
        } else if (stmt instanceof DropMaterializedViewStatement) {
            accessControl.checkCanDropTable(user, table.catalog(), table.schema(), table.table());
        } else if (stmt instanceof RefreshMaterializedViewStatement
                || stmt instanceof org.pangrova.parser.ast.statement.AlterMaterializedViewStatement) {
            accessControl.checkCanAlterTable(user, table.catalog(), table.schema(), table.table());
        }
    }

    private static boolean checkCatalogOrSchema(Statement stmt, CoordinatorSessionContext context,
            AccessControl accessControl, UserIdentity user) {
        if (stmt instanceof CreateCatalogStatement create) {
            accessControl.checkCanCreateCatalog(user, create.name());
            return true;
        }
        if (stmt instanceof DropCatalogStatement drop) {
            accessControl.checkCanDropCatalog(user, drop.name());
            return true;
        }
        if (stmt instanceof CreateSchemaStatement create) {
            var name = create.schemaName().parts();
            if (name.isEmpty() || name.size() > 2) {
                throw new IllegalArgumentException("Invalid schema name: " + name);
            }
            String catalog = name.size() == 2 ? name.get(0) : TableResolver.defaultCatalog(context);
            accessControl.checkCanCreateSchema(user, catalog, name.get(name.size() - 1));
            return true;
        }
        return false;
    }

    private static void checkDml(Statement stmt, UserIdentity user,
                                  AccessControl ac, String c, String s, String t) {
        if (stmt instanceof DeleteStatement) {
            ac.checkCanDeleteFromTable(user, c, s, t);
        } else if (stmt instanceof UpdateStatement) {
            ac.checkCanUpdateTable(user, c, s, t);
        } else if (stmt instanceof MergeStatement merge) {
            for (var clause : merge.clauses()) {
                if (clause instanceof MergeStatement.MergeClause.WhenMatchedUpdate) {
                    ac.checkCanUpdateTable(user, c, s, t);
                }
                if (clause instanceof MergeStatement.MergeClause.WhenMatchedDelete) {
                    ac.checkCanDeleteFromTable(user, c, s, t);
                }
                if (clause instanceof MergeStatement.MergeClause.WhenNotMatchedInsert) {
                    ac.checkCanInsertIntoTable(user, c, s, t);
                }
            }
        } else {
            ac.checkCanInsertIntoTable(user, c, s, t);
        }
    }

    private static void checkDdl(Statement stmt, UserIdentity user,
                                  AccessControl ac, QualifiedTableName source) {
        String c = source.catalog();
        String s = source.schema();
        String t = source.table();
        if (stmt instanceof DropTableStatement) {
            ac.checkCanDropTable(user, c, s, t);
        } else if (stmt instanceof CreateTableStatement || stmt instanceof CreateTableAsSelectStatement) {
            ac.checkCanCreateTable(user, c, s, t);
        } else if (stmt instanceof AlterTableStatement.RenameTable rename) {
            var target = TableResolver.resolveRenameTarget(rename.newName().parts(), source);
            ac.checkCanRenameTable(user, c, s, t, target.catalog(), target.schema(), target.table());
        } else {
            ac.checkCanAlterTable(user, c, s, t);
        }
    }
}

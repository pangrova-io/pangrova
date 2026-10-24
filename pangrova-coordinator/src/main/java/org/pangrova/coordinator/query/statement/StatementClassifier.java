package org.pangrova.coordinator.query.statement;

import org.pangrova.parser.ast.CreateCatalogStatement;
import org.pangrova.parser.ast.CreateContinuousQueryStatement;
import org.pangrova.parser.ast.CreateDynamicTableStatement;
import org.pangrova.parser.ast.CreateMaterializedViewStatement;
import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.InsertStatement;
import org.pangrova.parser.ast.ShowSchemasStatement;
import org.pangrova.parser.ast.Statement;
import org.pangrova.parser.ast.statement.AlterCatalogStatement;
import org.pangrova.parser.ast.statement.AlterDynamicTableStatement;
import org.pangrova.parser.ast.statement.AlterTableStatement;
import org.pangrova.parser.ast.statement.CreateSchemaStatement;
import org.pangrova.parser.ast.statement.CreateTableStatement;
import org.pangrova.parser.ast.statement.dml.DeleteStatement;
import org.pangrova.parser.ast.statement.DescribeDynamicTableStatement;
import org.pangrova.parser.ast.statement.DescribeMaterializedViewStatement;
import org.pangrova.parser.ast.statement.DropCatalogStatement;
import org.pangrova.parser.ast.statement.DropDynamicTableStatement;
import org.pangrova.parser.ast.statement.DropMaterializedViewStatement;
import org.pangrova.parser.ast.statement.DropTableStatement;
import org.pangrova.parser.ast.statement.RefreshMaterializedViewStatement;
import org.pangrova.parser.ast.statement.ShowMaterializationDependenciesStatement;
import org.pangrova.parser.ast.statement.ShowMaterializationGcHistoryStatement;
import org.pangrova.parser.ast.statement.ShowMaterializationRefreshHistoryStatement;
import org.pangrova.parser.ast.statement.ShowMaterializationsStatement;
import org.pangrova.parser.ast.statement.ShowMaterializedViewsStatement;
import org.pangrova.parser.ast.statement.ShowTablesStatement;
import org.pangrova.parser.ast.statement.dml.InsertOverwriteStatement;
import org.pangrova.parser.ast.statement.dml.InsertSelectStatement;
import org.pangrova.parser.ast.statement.dml.MergeStatement;
import org.pangrova.parser.ast.statement.dml.UpdateStatement;
import org.pangrova.parser.ast.statement.maintenance.CallStatement;
import org.pangrova.parser.ast.statement.maintenance.CompactStatement;
import org.pangrova.parser.ast.statement.maintenance.OptimizeStatement;
import org.pangrova.parser.ast.statement.maintenance.VacuumStatement;
import org.pangrova.spi.connector.metadata.table.QualifiedTableName;

import java.util.Map;
import java.util.function.Function;

/**
 * Classifies parsed statements by category (DDL, DML, show, maintenance, etc.)
 * and extracts table name identifiers for access control resolution.
 */
public final class StatementClassifier {

    private StatementClassifier() {
    }

    /** Returns whether a statement changes catalog, table, branch, or AI metadata. */
    public static boolean isDdl(Statement stmt) {
        return isCoreDdl(stmt) || isSemanticViewDdl(stmt)
                || isFeatureViewDdl(stmt) || isFeatureMaterializationDdl(stmt);
    }

    private static boolean isCoreDdl(Statement stmt) {
        return stmt instanceof CreateTableStatement
                || stmt instanceof org.pangrova.parser.ast.statement.CreateTableAsSelectStatement
                || stmt instanceof DropTableStatement
                || stmt instanceof AlterTableStatement
                || stmt instanceof CreateSchemaStatement
                || stmt instanceof CreateCatalogStatement
                || stmt instanceof DropCatalogStatement
                || stmt instanceof org.pangrova.parser.ast.SetSessionStatement
                || stmt instanceof org.pangrova.parser.ast.statement.CreateBranchStatement
                || stmt instanceof org.pangrova.parser.ast.statement.DropBranchStatement
                || stmt instanceof org.pangrova.parser.ast.statement.CreateTagStatement
                || stmt instanceof org.pangrova.parser.ast.statement.DropTagStatement;
    }

    private static boolean isSemanticViewDdl(Statement stmt) {
        return stmt instanceof org.pangrova.parser.ast.statement.CreateSemanticViewStatement
                || stmt instanceof org.pangrova.parser.ast.statement.PublishSemanticViewStatement;
    }

    private static boolean isFeatureViewDdl(Statement stmt) {
        return stmt instanceof org.pangrova.parser.ast.statement.CreateFeatureViewStatement
                || stmt instanceof org.pangrova.parser.ast.statement.PublishFeatureViewStatement;
    }

    private static boolean isFeatureMaterializationDdl(Statement stmt) {
        return stmt instanceof org.pangrova.parser.ast.statement.MaterializeFeatureViewStatement;
    }

    /** Returns whether a statement writes table rows. */
    public static boolean isDml(Statement stmt) {
        return stmt instanceof InsertStatement
                || stmt instanceof InsertOverwriteStatement
                || stmt instanceof InsertSelectStatement
                || stmt instanceof DeleteStatement
                || stmt instanceof UpdateStatement
                || stmt instanceof MergeStatement;
    }

    /** Returns whether a statement reads descriptive catalog metadata. */
    public static boolean isShow(Statement stmt) {
        return stmt instanceof ShowSchemasStatement
                || stmt instanceof ShowTablesStatement
                || stmt instanceof org.pangrova.parser.ast.statement.ShowTableControlsStatement
                || stmt instanceof org.pangrova.parser.ast.statement.DescribeTableStatement
                || stmt instanceof org.pangrova.parser.ast.statement.ShowCatalogsStatement
                || stmt instanceof org.pangrova.parser.ast.statement.ShowSemanticViewsStatement
                || stmt instanceof ShowMaterializationsStatement
                || stmt instanceof ShowMaterializationRefreshHistoryStatement
                || stmt instanceof ShowMaterializationDependenciesStatement
                || stmt instanceof ShowMaterializationGcHistoryStatement;
    }

    /** Returns whether a statement is a scheduled maintenance operation. */
    public static boolean isMaintenance(Statement stmt) {
        return stmt instanceof CompactStatement
                || stmt instanceof OptimizeStatement
                || stmt instanceof VacuumStatement
                || stmt instanceof org.pangrova.parser.ast.statement.maintenance.AnalyzeStatement;
    }

    /** Returns whether a statement manages a dynamic table or continuous query. */
    public static boolean isDynamicTable(Statement stmt) {
        return stmt instanceof CreateDynamicTableStatement
                || stmt instanceof CreateContinuousQueryStatement
                || stmt instanceof AlterDynamicTableStatement
                || stmt instanceof DropDynamicTableStatement
                || stmt instanceof org.pangrova.parser.ast.statement.ShowDynamicTablesStatement
                || stmt instanceof DescribeDynamicTableStatement;
    }

    /** Returns whether a statement manages a materialized view. */
    public static boolean isMaterializedView(Statement stmt) {
        return stmt instanceof CreateMaterializedViewStatement
                || stmt instanceof org.pangrova.parser.ast.statement.AlterMaterializedViewStatement
                || stmt instanceof RefreshMaterializedViewStatement
                || stmt instanceof DropMaterializedViewStatement
                || stmt instanceof ShowMaterializedViewsStatement
                || stmt instanceof DescribeMaterializedViewStatement;
    }

    public static boolean isUnsupported(Statement stmt) {
        return stmt instanceof org.pangrova.parser.ast.statement.DescribeCatalogStatement
                || stmt instanceof AlterCatalogStatement
                || stmt instanceof CallStatement;
    }

    /** Returns whether a statement manages an index. */
    public static boolean isIndex(Statement stmt) {
        return stmt instanceof org.pangrova.parser.ast.statement.CreateIndexStatement
                || stmt instanceof org.pangrova.parser.ast.statement.DropIndexStatement
                || stmt instanceof org.pangrova.parser.ast.statement.ShowIndexesStatement
                || stmt instanceof org.pangrova.parser.ast.statement.AlterIndexStatement;
    }

    public static boolean isClustering(Statement stmt) {
        return stmt instanceof org.pangrova.parser.ast.statement.SetClusteringOrderStatement
                || stmt instanceof org.pangrova.parser.ast.statement.ReclusterTableStatement
                || stmt instanceof org.pangrova.parser.ast.statement.ShowClusteringStatusStatement;
    }

    public static boolean isTenant(Statement stmt) {
        return stmt instanceof org.pangrova.parser.ast.statement.GrantTenantStatement
                || stmt instanceof org.pangrova.parser.ast.statement.RevokeTenantStatement;
    }

    /** Returns whether a statement manages a stream. */
    public static boolean isStream(Statement stmt) {
        return stmt instanceof org.pangrova.parser.ast.statement.stream.CreateStreamStatement
                || stmt instanceof org.pangrova.parser.ast.statement.stream.AlterStreamStatement
                || stmt instanceof org.pangrova.parser.ast.statement.stream.DropStreamStatement
                || stmt instanceof org.pangrova.parser.ast.statement.stream.ShowStreamsStatement
                || stmt instanceof org.pangrova.parser.ast.statement.stream.DescribeStreamStatement;
    }

    /** Returns true if the statement requires DDL audit logging. */
    public static boolean requiresDdlAudit(Statement stmt) {
        return isDdl(stmt) || isIndex(stmt) || isClustering(stmt)
                || isMaterializedView(stmt);
    }

    /** Returns true if the statement must be routed to the leader coordinator. */
    public static boolean requiresLeaderRouting(Statement stmt) {
        return isDdl(stmt) || isIndex(stmt) || isClustering(stmt)
                || isDynamicTable(stmt) || isMaterializedView(stmt)
                || isStream(stmt) || isTenant(stmt);
    }

    private static final Map<Class<? extends Statement>, Function<Statement, Identifier>> TABLE_NAME_EXTRACTORS =
            Map.ofEntries(
                    Map.entry(CreateTableStatement.class, s -> ((CreateTableStatement) s).tableName()),
                    Map.entry(DropTableStatement.class, s -> ((DropTableStatement) s).tableName()),
                    Map.entry(AlterTableStatement.AddColumn.class, s -> ((AlterTableStatement) s).tableName()),
                    Map.entry(AlterTableStatement.DropColumn.class, s -> ((AlterTableStatement) s).tableName()),
                    Map.entry(AlterTableStatement.RenameColumn.class, s -> ((AlterTableStatement) s).tableName()),
                    Map.entry(AlterTableStatement.ChangeColumnType.class, s -> ((AlterTableStatement) s).tableName()),
                    Map.entry(AlterTableStatement.SetPartitionSpec.class, s -> ((AlterTableStatement) s).tableName()),
                    Map.entry(InsertStatement.class, s -> ((InsertStatement) s).tableName()),
                    Map.entry(InsertOverwriteStatement.class, s -> ((InsertOverwriteStatement) s).tableName()),
                    Map.entry(InsertSelectStatement.class, s -> ((InsertSelectStatement) s).tableName()),
                    Map.entry(DeleteStatement.class, s -> ((DeleteStatement) s).tableName()),
                    Map.entry(UpdateStatement.class, s -> ((UpdateStatement) s).tableName()),
                    Map.entry(MergeStatement.class, s -> ((MergeStatement) s).targetTable()),
                    Map.entry(org.pangrova.parser.ast.statement.CreateIndexStatement.class,
                            s -> ((org.pangrova.parser.ast.statement.CreateIndexStatement) s).tableName()),
                    Map.entry(org.pangrova.parser.ast.statement.DropIndexStatement.class,
                            s -> ((org.pangrova.parser.ast.statement.DropIndexStatement) s).tableName()),
                    Map.entry(org.pangrova.parser.ast.statement.AlterIndexStatement.class,
                            s -> ((org.pangrova.parser.ast.statement.AlterIndexStatement) s).tableName()),
                    Map.entry(org.pangrova.parser.ast.statement.SetClusteringOrderStatement.class,
                            s -> ((org.pangrova.parser.ast.statement.SetClusteringOrderStatement) s).tableName()),
                    Map.entry(org.pangrova.parser.ast.statement.ReclusterTableStatement.class,
                            s -> ((org.pangrova.parser.ast.statement.ReclusterTableStatement) s).tableName()),
                    Map.entry(CreateDynamicTableStatement.class, s -> ((CreateDynamicTableStatement) s).name()),
                    Map.entry(DropDynamicTableStatement.class, s -> ((DropDynamicTableStatement) s).name()),
                    Map.entry(AlterDynamicTableStatement.class, s -> ((AlterDynamicTableStatement) s).name()),
                    Map.entry(CreateMaterializedViewStatement.class,
                            s -> ((CreateMaterializedViewStatement) s).name()),
                    Map.entry(org.pangrova.parser.ast.statement.AlterMaterializedViewStatement.class,
                            s -> ((org.pangrova.parser.ast.statement.AlterMaterializedViewStatement) s)
                                    .name()),
                    Map.entry(RefreshMaterializedViewStatement.class,
                            s -> ((RefreshMaterializedViewStatement) s).name()),
                    Map.entry(DropMaterializedViewStatement.class,
                            s -> ((DropMaterializedViewStatement) s).name()),
                    Map.entry(DescribeMaterializedViewStatement.class,
                            s -> ((DescribeMaterializedViewStatement) s).name())
            );

    /** Extracts the affected table identifier when the statement names one. */
    public static Identifier extractTableName(Statement stmt) {
        if (stmt instanceof AlterTableStatement alter) {
            return alter.tableName();
        }
        if (stmt instanceof org.pangrova.parser.ast.statement.CreateTableAsSelectStatement ctas) {
            return ctas.tableName();
        }
        Function<Statement, Identifier> extractor = TABLE_NAME_EXTRACTORS.get(stmt.getClass());
        return extractor != null ? extractor.apply(stmt) : null;
    }

    public static QualifiedTableName extractTableParts(
            Statement stmt, org.pangrova.coordinator.query.session.CoordinatorSessionContext context) {
        Identifier name = extractTableName(stmt);
        if (name == null) return null;
        return TableResolver.resolveTableRef(name.parts(), context);
    }
}

package org.pangrova.coordinator.query.execution.dispatch;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;

import org.pangrova.parser.ast.CreateContinuousQueryStatement;
import org.pangrova.parser.ast.CreateDynamicTableStatement;
import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.InsertStatement;
import org.pangrova.parser.ast.Statement;
import org.pangrova.parser.ast.statement.AlterDynamicTableStatement;
import org.pangrova.parser.ast.statement.AlterIndexStatement;
import org.pangrova.parser.ast.statement.AlterTableStatement;
import org.pangrova.parser.ast.statement.CreateIndexStatement;
import org.pangrova.parser.ast.statement.CreateTableAsSelectStatement;
import org.pangrova.parser.ast.statement.CreateTableStatement;
import org.pangrova.parser.ast.statement.DropDynamicTableStatement;
import org.pangrova.parser.ast.statement.DropIndexStatement;
import org.pangrova.parser.ast.statement.DropTableStatement;
import org.pangrova.parser.ast.statement.ReclusterTableStatement;
import org.pangrova.parser.ast.statement.SetClusteringOrderStatement;
import org.pangrova.parser.ast.statement.dml.DeleteStatement;
import org.pangrova.parser.ast.statement.dml.InsertOverwriteStatement;
import org.pangrova.parser.ast.statement.dml.InsertSelectStatement;
import org.pangrova.parser.ast.statement.dml.MergeStatement;
import org.pangrova.parser.ast.statement.dml.UpdateStatement;
import org.pangrova.spi.connector.metadata.table.QualifiedTableName;

/**
 * Extracts the mutated table name from a statement AST node.
 * Uses a registry-based approach instead of instanceof chains.
 */
public final class TableNameExtractor {

    private static final Map<Class<? extends Statement>, Function<Statement, Identifier>>
            EXTRACTORS = buildExtractorMap();

    private TableNameExtractor() {}

    /** Returns the mutated table FQN (lowercase), or null if not applicable. */
    public static String extract(Statement stmt) {
        if (stmt == null) {
            return null;
        }
        var extractor = EXTRACTORS.get(stmt.getClass());
        if (extractor == null) {
            return null;
        }
        Identifier id = extractor.apply(stmt);
        return id != null ? String.join(".", id.parts()).toLowerCase() : null;
    }

    /**
     * Resolves the mutated table as a QualifiedTableName (catalog.schema.table),
     * or null if the statement does not target a named table.
     * Used by permission checks where catalog/schema/table must be separate.
     */
    public static QualifiedTableName extractQualified(
            Statement stmt, org.pangrova.coordinator.query.session.CoordinatorSessionContext context) {
        if (stmt == null) {
            return null;
        }
        var extractor = EXTRACTORS.get(stmt.getClass());
        if (extractor == null) {
            return null;
        }
        Identifier id = extractor.apply(stmt);
        return id != null ? org.pangrova.coordinator.query.statement.TableResolver
                .resolveTableRef(id.parts(), context) : null;
    }

    private static Map<Class<? extends Statement>, Function<Statement, Identifier>>
            buildExtractorMap() {
        var map = new LinkedHashMap<Class<? extends Statement>, Function<Statement, Identifier>>();
        put(map, CreateTableAsSelectStatement.class, CreateTableAsSelectStatement::tableName);
        put(map, CreateTableStatement.class, CreateTableStatement::tableName);
        put(map, DropTableStatement.class, DropTableStatement::tableName);
        put(map, AlterTableStatement.AddColumn.class, AlterTableStatement.AddColumn::tableName);
        put(map, AlterTableStatement.DropColumn.class, AlterTableStatement.DropColumn::tableName);
        put(map, AlterTableStatement.RenameColumn.class, AlterTableStatement.RenameColumn::tableName);
        put(map, AlterTableStatement.ChangeColumnType.class, AlterTableStatement.ChangeColumnType::tableName);
        put(map, AlterTableStatement.RenameTable.class, AlterTableStatement.RenameTable::tableName);
        put(map, AlterTableStatement.SetPartitionSpec.class, AlterTableStatement.SetPartitionSpec::tableName);
        put(map, AlterTableStatement.SetSortOrder.class, AlterTableStatement.SetSortOrder::tableName);
        put(map, AlterTableStatement.CreateBranch.class, AlterTableStatement.CreateBranch::tableName);
        put(map, AlterTableStatement.DropBranch.class, AlterTableStatement.DropBranch::tableName);
        put(map, AlterTableStatement.CreateTag.class, AlterTableStatement.CreateTag::tableName);
        put(map, AlterTableStatement.DropTag.class, AlterTableStatement.DropTag::tableName);
        put(map, AlterTableStatement.SetColumnTag.class, AlterTableStatement.SetColumnTag::tableName);
        put(map, AlterTableStatement.UnsetColumnTag.class, AlterTableStatement.UnsetColumnTag::tableName);
        put(map, InsertStatement.class, InsertStatement::tableName);
        put(map, InsertOverwriteStatement.class, InsertOverwriteStatement::tableName);
        put(map, InsertSelectStatement.class, InsertSelectStatement::tableName);
        put(map, DeleteStatement.class, DeleteStatement::tableName);
        put(map, UpdateStatement.class, UpdateStatement::tableName);
        put(map, MergeStatement.class, MergeStatement::targetTable);
        put(map, CreateIndexStatement.class, CreateIndexStatement::tableName);
        put(map, DropIndexStatement.class, DropIndexStatement::tableName);
        put(map, AlterIndexStatement.class, AlterIndexStatement::tableName);
        put(map, SetClusteringOrderStatement.class, SetClusteringOrderStatement::tableName);
        put(map, ReclusterTableStatement.class, ReclusterTableStatement::tableName);
        put(map, CreateDynamicTableStatement.class, CreateDynamicTableStatement::name);
        put(map, CreateContinuousQueryStatement.class, CreateContinuousQueryStatement::name);
        put(map, DropDynamicTableStatement.class, DropDynamicTableStatement::name);
        put(map, AlterDynamicTableStatement.class, AlterDynamicTableStatement::name);
        put(map, org.pangrova.parser.ast.CreateMaterializedViewStatement.class,
                org.pangrova.parser.ast.CreateMaterializedViewStatement::name);
        put(map, org.pangrova.parser.ast.statement.AlterMaterializedViewStatement.class,
                org.pangrova.parser.ast.statement.AlterMaterializedViewStatement::name);
        put(map, org.pangrova.parser.ast.statement.RefreshMaterializedViewStatement.class,
                org.pangrova.parser.ast.statement.RefreshMaterializedViewStatement::name);
        put(map, org.pangrova.parser.ast.statement.DropMaterializedViewStatement.class,
                org.pangrova.parser.ast.statement.DropMaterializedViewStatement::name);
        put(map, org.pangrova.parser.ast.statement.DescribeMaterializedViewStatement.class,
                org.pangrova.parser.ast.statement.DescribeMaterializedViewStatement::name);
        put(map, org.pangrova.parser.ast.statement.CreateBranchStatement.class,
                s -> ((org.pangrova.parser.ast.statement.CreateBranchStatement) s).tableName());
        put(map, org.pangrova.parser.ast.statement.DropBranchStatement.class,
                s -> ((org.pangrova.parser.ast.statement.DropBranchStatement) s).tableName());
        put(map, org.pangrova.parser.ast.statement.CreateTagStatement.class,
                s -> ((org.pangrova.parser.ast.statement.CreateTagStatement) s).tableName());
        put(map, org.pangrova.parser.ast.statement.DropTagStatement.class,
                s -> ((org.pangrova.parser.ast.statement.DropTagStatement) s).tableName());
        put(map, org.pangrova.parser.ast.statement.maintenance.AnalyzeStatement.class,
                org.pangrova.parser.ast.statement.maintenance.AnalyzeStatement::tableName);
        return Map.copyOf(map);
    }

    @SuppressWarnings("unchecked")
    private static <T extends Statement> void put(
            Map<Class<? extends Statement>, Function<Statement, Identifier>> map,
            Class<T> type, Function<T, Identifier> fn) {
        map.put(type, (Function<Statement, Identifier>) (Function<?, ?>) fn);
    }
}

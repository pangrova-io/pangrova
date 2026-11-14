package org.pangrova.query.statement;

import org.pangrova.query.session.SessionTableNames;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;

import org.pangrova.parser.api.ast.CreateContinuousQueryStatement;
import org.pangrova.parser.api.ast.CreateDynamicTableStatement;
import org.pangrova.parser.api.ast.Identifier;
import org.pangrova.parser.api.ast.InsertStatement;
import org.pangrova.parser.api.ast.Statement;
import org.pangrova.parser.api.ast.statement.AlterDynamicTableStatement;
import org.pangrova.parser.api.ast.statement.AlterIndexStatement;
import org.pangrova.parser.api.ast.statement.AlterTableStatement;
import org.pangrova.parser.api.ast.statement.CreateIndexStatement;
import org.pangrova.parser.api.ast.statement.CreateTableAsSelectStatement;
import org.pangrova.parser.api.ast.statement.CreateTableStatement;
import org.pangrova.parser.api.ast.statement.DropDynamicTableStatement;
import org.pangrova.parser.api.ast.statement.DropIndexStatement;
import org.pangrova.parser.api.ast.statement.DropTableStatement;
import org.pangrova.parser.api.ast.statement.ReclusterTableStatement;
import org.pangrova.parser.api.ast.statement.SetClusteringOrderStatement;
import org.pangrova.parser.api.ast.statement.dml.DeleteStatement;
import org.pangrova.parser.api.ast.statement.dml.InsertOverwriteStatement;
import org.pangrova.parser.api.ast.statement.dml.InsertSelectStatement;
import org.pangrova.parser.api.ast.statement.dml.MergeStatement;
import org.pangrova.parser.api.ast.statement.dml.UpdateStatement;
import org.pangrova.connector.api.metadata.table.QualifiedTableName;

/**
 * Extracts the mutated table name from a statement AST node.
 * Uses a registry-based approach instead of instanceof chains.
 */
public final class StatementTableTargets {

    private static final Map<Class<? extends Statement>, Function<Statement, Identifier>>
            EXTRACTORS = buildExtractorMap();

    private StatementTableTargets() {}

    /** Returns the exact AST target, or null for statements with no table target. */
    public static Identifier identifier(Statement statement) {
        if (statement == null) return null;
        if (statement instanceof AlterTableStatement alter) return alter.tableName();
        var extractor = EXTRACTORS.get(statement.getClass());
        return extractor == null ? null : extractor.apply(statement);
    }

    /** Qualifies the same exact target used by permission checks and statement completion. */
    public static QualifiedTableName resolve(
            Statement statement, org.pangrova.query.api.session.QuerySession session) {
        if (statement instanceof org.pangrova.parser.api.ast.statement.maintenance.CallStatement call) {
            return TableProcedureCall.bind(call).target();
        }
        Identifier target = identifier(statement);
        return target == null ? null : SessionTableNames.resolveTableRef(target.parts(), session);
    }

    private static Map<Class<? extends Statement>, Function<Statement, Identifier>>
            buildExtractorMap() {
        var map = new LinkedHashMap<Class<? extends Statement>, Function<Statement, Identifier>>();
        put(map, CreateTableAsSelectStatement.class, CreateTableAsSelectStatement::tableName);
        put(map, CreateTableStatement.class, CreateTableStatement::tableName);
        put(map, org.pangrova.parser.api.ast.statement.CreateViewStatement.class,
                org.pangrova.parser.api.ast.statement.CreateViewStatement::viewName);
        put(map, org.pangrova.parser.api.ast.statement.DropViewStatement.class,
                org.pangrova.parser.api.ast.statement.DropViewStatement::viewName);
        put(map, DropTableStatement.class, DropTableStatement::tableName);
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
        put(map, org.pangrova.parser.api.ast.CreateMaterializedViewStatement.class,
                org.pangrova.parser.api.ast.CreateMaterializedViewStatement::name);
        put(map, org.pangrova.parser.api.ast.statement.AlterMaterializedViewStatement.class,
                org.pangrova.parser.api.ast.statement.AlterMaterializedViewStatement::name);
        put(map, org.pangrova.parser.api.ast.statement.RefreshMaterializedViewStatement.class,
                org.pangrova.parser.api.ast.statement.RefreshMaterializedViewStatement::name);
        put(map, org.pangrova.parser.api.ast.statement.DropMaterializedViewStatement.class,
                org.pangrova.parser.api.ast.statement.DropMaterializedViewStatement::name);
        put(map, org.pangrova.parser.api.ast.statement.DescribeMaterializedViewStatement.class,
                org.pangrova.parser.api.ast.statement.DescribeMaterializedViewStatement::name);
        put(map, org.pangrova.parser.api.ast.statement.CreateBranchStatement.class,
                s -> ((org.pangrova.parser.api.ast.statement.CreateBranchStatement) s).tableName());
        put(map, org.pangrova.parser.api.ast.statement.DropBranchStatement.class,
                s -> ((org.pangrova.parser.api.ast.statement.DropBranchStatement) s).tableName());
        put(map, org.pangrova.parser.api.ast.statement.CreateTagStatement.class,
                s -> ((org.pangrova.parser.api.ast.statement.CreateTagStatement) s).tableName());
        put(map, org.pangrova.parser.api.ast.statement.DropTagStatement.class,
                s -> ((org.pangrova.parser.api.ast.statement.DropTagStatement) s).tableName());
        put(map, org.pangrova.parser.api.ast.statement.maintenance.AnalyzeStatement.class,
                org.pangrova.parser.api.ast.statement.maintenance.AnalyzeStatement::tableName);
        put(map, org.pangrova.parser.api.ast.statement.maintenance.CompactStatement.class,
                org.pangrova.parser.api.ast.statement.maintenance.CompactStatement::tableName);
        put(map, org.pangrova.parser.api.ast.statement.maintenance.OptimizeStatement.class,
                org.pangrova.parser.api.ast.statement.maintenance.OptimizeStatement::tableName);
        put(map, org.pangrova.parser.api.ast.statement.maintenance.VacuumStatement.class,
                org.pangrova.parser.api.ast.statement.maintenance.VacuumStatement::tableName);
        return Map.copyOf(map);
    }

    @SuppressWarnings("unchecked")
    private static <T extends Statement> void put(
            Map<Class<? extends Statement>, Function<Statement, Identifier>> map,
            Class<T> type, Function<T, Identifier> fn) {
        map.put(type, (Function<Statement, Identifier>) (Function<?, ?>) fn);
    }
}

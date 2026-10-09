package org.pangrova.planner.parse;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.planner.PlannerException;
import org.pangrova.parser.ast.CreateCatalogStatement;
import org.pangrova.parser.ast.Expression;
import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.InsertStatement;
import org.pangrova.parser.ast.ShowSchemasStatement;
import org.pangrova.parser.ast.Statement;
import org.pangrova.parser.ast.statement.AlterCatalogStatement;
import org.pangrova.parser.ast.statement.AlterTableStatement;
import org.pangrova.parser.ast.statement.CreateSchemaStatement;
import org.pangrova.parser.ast.statement.CreateTableStatement;
import org.pangrova.parser.ast.statement.dml.DeleteStatement;
import org.pangrova.parser.ast.statement.maintenance.CompactStatement;
import org.pangrova.parser.ast.statement.DescribeCatalogStatement;
import org.pangrova.parser.ast.statement.DescribeTableStatement;
import org.pangrova.parser.ast.statement.DropCatalogStatement;
import org.pangrova.parser.ast.statement.DropTableStatement;
import org.pangrova.parser.ast.statement.dml.MergeStatement;
import org.pangrova.parser.ast.statement.ShowCatalogsStatement;
import org.pangrova.parser.ast.statement.ShowTablesStatement;
import org.pangrova.parser.ast.statement.dml.InsertOverwriteStatement;
import org.pangrova.parser.ast.statement.dml.InsertSelectStatement;
import org.pangrova.parser.ast.statement.dml.UpdateStatement;
import org.pangrova.parser.ast.statement.maintenance.CallStatement;
import org.pangrova.parser.ast.statement.maintenance.OptimizeStatement;
import org.pangrova.parser.ast.statement.maintenance.VacuumStatement;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlMerge;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Converts Pangrova extension statements (DDL, DML, maintenance, catalog)
 * to Calcite SqlNode trees. Dispatch uses a type-keyed map to keep branch
 * count per method low.
 */
final class ExtensionStatementSqlNodeConverter {

    private static final SqlParserPos POS = SqlParserPos.ZERO;

    private final AstToSqlNodeConverter converter;
    private final Map<Class<? extends Statement>, Function<Statement, SqlNode>> extensionDispatch =
            buildExtensionDispatch();

    ExtensionStatementSqlNodeConverter(AstToSqlNodeConverter converter) {
        this.converter = converter;
    }

    SqlNode convert(Statement stmt) {
        // Walk class hierarchy to support sealed interface subtypes (e.g. AlterTableStatement.AddColumn)
        Class<?> clazz = stmt.getClass();
        while (clazz != null) {
            Function<Statement, SqlNode> handler = extensionDispatch.get(clazz);
            if (handler != null) {
                return handler.apply(stmt);
            }
            for (Class<?> iface : clazz.getInterfaces()) {
                handler = extensionDispatch.get(iface);
                if (handler != null) {
                    return handler.apply(stmt);
                }
            }
            clazz = clazz.getSuperclass();
        }
        throw new IllegalArgumentException("Unknown statement type: " + stmt.getClass());
    }

    private Map<Class<? extends Statement>, Function<Statement, SqlNode>> buildExtensionDispatch() {
        Map<Class<? extends Statement>, Function<Statement, SqlNode>> map = new LinkedHashMap<>();
        addCatalogAndMaintenanceHandlers(map);
        addDdlHandlers(map);
        addDmlHandlers(map);
        addMetadataQueryHandlers(map);
        return Map.copyOf(map);
    }

    private void addCatalogAndMaintenanceHandlers(
            Map<Class<? extends Statement>, Function<Statement, SqlNode>> map) {
        map.put(CompactStatement.class,
            s -> ext(PangrovaOperatorTable.COMPACT, id(((CompactStatement) s).tableName())));
        map.put(OptimizeStatement.class,
                s -> ext(PangrovaOperatorTable.OPTIMIZE, id(((OptimizeStatement) s).tableName())));
        map.put(VacuumStatement.class, s -> ext(PangrovaOperatorTable.VACUUM, id(((VacuumStatement) s).tableName())));
        map.put(CreateCatalogStatement.class,
                s -> ext(PangrovaOperatorTable.CREATE_CATALOG, str(((CreateCatalogStatement) s).name())));
        map.put(DropCatalogStatement.class,
                s -> ext(PangrovaOperatorTable.DROP_CATALOG, str(((DropCatalogStatement) s).name())));
        map.put(ShowCatalogsStatement.class, s -> ext(PangrovaOperatorTable.SHOW_CATALOGS));
        map.put(DescribeCatalogStatement.class,
                s -> ext(PangrovaOperatorTable.DESCRIBE_CATALOG, str(((DescribeCatalogStatement) s).name())));
        map.put(AlterCatalogStatement.class,
                s -> ext(PangrovaOperatorTable.ALTER_CATALOG, str(((AlterCatalogStatement) s).name())));
        map.put(CallStatement.class, s -> convertCall((CallStatement) s));
    }

    private void addDdlHandlers(Map<Class<? extends Statement>, Function<Statement, SqlNode>> map) {
        map.put(CreateTableStatement.class, s -> ext(PangrovaOperatorTable.CREATE_TABLE));
        map.put(DropTableStatement.class, s -> ext(PangrovaOperatorTable.DROP_TABLE));
        map.put(AlterTableStatement.class,
                s -> ext(PangrovaOperatorTable.ALTER_TABLE, id(((AlterTableStatement) s).tableName())));
        map.put(CreateSchemaStatement.class, s -> ext(
            PangrovaOperatorTable.CREATE_SCHEMA,
            str(((CreateSchemaStatement) s).schemaName().toString())));
    }

    private void addDmlHandlers(Map<Class<? extends Statement>, Function<Statement, SqlNode>> map) {
        map.put(InsertStatement.class, s -> convertInsertValues((InsertStatement) s));
        map.put(InsertSelectStatement.class, s -> convertInsertSelect((InsertSelectStatement) s));
        map.put(InsertOverwriteStatement.class,
                s -> convertInsertOverwriteValues((InsertOverwriteStatement) s));
        map.put(DeleteStatement.class, s -> convertDelete((DeleteStatement) s));
        map.put(UpdateStatement.class, s -> convertUpdate((UpdateStatement) s));
        map.put(MergeStatement.class, s -> convertMerge((MergeStatement) s));
    }

    private void addMetadataQueryHandlers(Map<Class<? extends Statement>, Function<Statement, SqlNode>> map) {
        map.put(ShowSchemasStatement.class, s -> ext(PangrovaOperatorTable.SHOW_SCHEMAS));
        map.put(ShowTablesStatement.class, s -> ext(PangrovaOperatorTable.SHOW_TABLES));
        map.put(DescribeTableStatement.class,
                s -> ext(PangrovaOperatorTable.DESCRIBE_TABLE, id(((DescribeTableStatement) s).tableName())));
    }

    private SqlNode convertDelete(DeleteStatement d) {
        SqlNode table = id(d.tableName());
        SqlNode cond = d.where().map(this::convertExpression).orElse(null);
        SqlSelect src = new SqlSelect(POS, null, new SqlNodeList(List.of(SqlIdentifier.star(POS)), POS),
                table, cond, null, null, null, null, null, null, null);
        return new SqlDelete(POS, table, cond, src, null);
    }

    private SqlNode convertUpdate(UpdateStatement u) {
        SqlNode table = id(u.tableName());
        SqlNode cond = u.where().map(this::convertExpression).orElse(null);
        SqlNodeList cols = new SqlNodeList(u.assignments().stream()
                .map(a -> (SqlNode) new SqlIdentifier(a.column(), POS)).toList(), POS);
        SqlNodeList vals = new SqlNodeList(u.assignments().stream()
                .map(a -> convertExpression(a.value())).toList(), POS);
        SqlSelect src = new SqlSelect(POS, null, new SqlNodeList(List.of(SqlIdentifier.star(POS)), POS),
                table, cond, null, null, null, null, null, null, null);
        return new SqlUpdate(POS, table, cols, vals, cond, src, null);
    }

    private SqlNode convertMerge(MergeStatement m) {
        validateMergeActions(m);
        SqlIdentifier target = (SqlIdentifier) id(m.targetTable());
        SqlIdentifier targetAlias = m.targetAlias()
                .map(a -> new SqlIdentifier(a, POS)).orElse(null);
        // Source: SqlIdentifier optionally wrapped with AS for alias
        SqlNode sourceId = id(m.sourceTable().name());
        SqlNode source = m.sourceTable().alias()
                .map(a -> (SqlNode) SqlStdOperatorTable.AS.createCall(POS, sourceId, new SqlIdentifier(a, POS)))
                .orElse(sourceId);
        SqlNode onCond = convertExpression(m.onCondition());
        SqlUpdate upd = null;
        SqlInsert ins = null;
        for (var clause : m.clauses()) {
            if (clause instanceof MergeStatement.MergeClause.WhenMatchedUpdate wu) {
                onCond = matchedCondition(onCond, wu.condition());
                SqlNodeList c = new SqlNodeList(wu.assignments().stream()
                        .map(a -> (SqlNode) new SqlIdentifier(a.column(), POS)).toList(), POS);
                SqlNodeList v = new SqlNodeList(wu.assignments().stream()
                        .map(a -> convertExpression(a.value())).toList(), POS);
                upd = new SqlUpdate(POS, target, c, v,
                        wu.condition().map(this::convertExpression).orElse(null), null, null);
            } else if (clause instanceof MergeStatement.MergeClause.WhenMatchedDelete wd) {
                onCond = matchedCondition(onCond, wd.condition());
                if (upd == null) {
                    upd = new SqlUpdate(POS, target, SqlNodeList.EMPTY, SqlNodeList.EMPTY,
                            wd.condition().map(this::convertExpression).orElse(null), null, null);
                }
            } else if (clause instanceof MergeStatement.MergeClause.WhenNotMatchedInsert wi) {
                SqlNodeList c2 = wi.columns().map(this::toColumnNodeList).orElse(null);
                SqlNode valsNode = SqlStdOperatorTable.VALUES.createCall(POS,
                        SqlStdOperatorTable.ROW.createCall(POS,
                                wi.values().stream().map(this::convertExpression).toArray(SqlNode[]::new)));
                ins = new SqlInsert(POS, SqlNodeList.EMPTY, target, valsNode, c2);
            }
        }
        return new SqlMerge(POS, target, onCond, source, upd, ins, null, targetAlias);
    }

    private SqlNode matchedCondition(SqlNode onCondition, java.util.Optional<Expression> actionCondition) {
        // With a single matched action, its guard can restrict the match join:
        // Native CoW lowering still retains every non-matching physical target.
        return actionCondition.map(condition -> (SqlNode) SqlStdOperatorTable.AND.createCall(
                POS, onCondition, convertExpression(condition))).orElse(onCondition);
    }

    private static void validateMergeActions(MergeStatement merge) {
        long matched = merge.clauses().stream()
                .filter(clause -> !(clause instanceof MergeStatement.MergeClause.WhenNotMatchedInsert)).count();
        long inserts = merge.clauses().size() - matched;
        if (matched > 1 || inserts > 1) {
            throw new PlannerException(PangrovaErrorCode.NATIVE_UNSUPPORTED_PLAN,
                    "Multiple MERGE actions require ordered action lowering; clauses cannot be discarded");
        }
        for (var clause : merge.clauses()) {
            if (clause instanceof MergeStatement.MergeClause.WhenNotMatchedInsert insert
                    && insert.condition().isPresent()) {
                throw new PlannerException(PangrovaErrorCode.NATIVE_UNSUPPORTED_PLAN,
                        "Conditional MERGE INSERT requires a separately compiled source action predicate");
            }
            boolean conditionalMatched = clause instanceof MergeStatement.MergeClause.WhenMatchedUpdate update
                    && update.condition().isPresent()
                    || clause instanceof MergeStatement.MergeClause.WhenMatchedDelete delete
                    && delete.condition().isPresent();
            if (conditionalMatched && merge.clauses().size() > 1) {
                throw new PlannerException(PangrovaErrorCode.NATIVE_UNSUPPORTED_PLAN,
                        "Conditional mixed MERGE requires distinct match and action predicates");
            }
        }
    }

    private SqlNode convertInsertSelect(InsertSelectStatement stmt) {
        SqlNode targetTable = id(stmt.tableName());
        SqlNode source = converter.convert(stmt.selectQuery());
        SqlNodeList columnList = stmt.columns()
                .map(this::toColumnNodeList).orElse(null);
        return new SqlInsert(POS, SqlNodeList.EMPTY, targetTable, source, columnList);
    }

    private SqlNode convertInsertValues(InsertStatement stmt) {
        return valuesInsert(stmt.tableName(), stmt.columns(), stmt.values());
    }

    private SqlNode convertInsertOverwriteValues(InsertOverwriteStatement stmt) {
        return valuesInsert(stmt.tableName(), stmt.columns(), stmt.values());
    }

    private SqlInsert valuesInsert(
            Identifier tableName,
            java.util.Optional<List<String>> columns,
            List<List<Expression>> rows) {
        if (rows.isEmpty()) {
            throw new IllegalArgumentException("INSERT VALUES requires at least one row");
        }
        SqlNode[] rowNodes = rows.stream()
                .map(row -> SqlStdOperatorTable.ROW.createCall(
                        POS, row.stream().map(this::convertExpression).toArray(SqlNode[]::new)))
                .toArray(SqlNode[]::new);
        SqlNode values = SqlStdOperatorTable.VALUES.createCall(POS, rowNodes);
        SqlNodeList columnList = columns.map(this::toColumnNodeList).orElse(null);
        return new SqlInsert(POS, SqlNodeList.EMPTY, id(tableName), values, columnList);
    }

    private SqlNode convertCall(CallStatement stmt) {
        SqlNode[] args = stmt.arguments().stream().map(this::convertExpression).toArray(SqlNode[]::new);
        return new SqlBasicCall(PangrovaOperatorTable.CALL, args, POS);
    }

    private SqlNodeList toColumnNodeList(List<String> cols) {
        List<SqlNode> ids = cols.stream()
                .map(c -> (SqlNode) new SqlIdentifier(c, POS)).toList();
        return new SqlNodeList(ids, POS);
    }

    private SqlNode convertExpression(Expression expr) {
        return converter.convertExpression(expr);
    }

    // ── Helpers ──

    private SqlNode ext(SqlFunction op, SqlNode... operands) { return new SqlBasicCall(op, operands, POS); }
    private SqlNode id(Identifier ident) { return new SqlIdentifier(ident.parts(), POS); }
    private SqlNode str(String value) { return SqlLiteral.createCharString(value, POS); }
}

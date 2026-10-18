package org.pangrova.planner.parse;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.planner.PlannerException;
import org.pangrova.parser.ast.AllColumns;
import org.pangrova.parser.ast.CteQuery;
import org.pangrova.parser.ast.CubeGroupBy;
import org.pangrova.parser.ast.Expression;
import org.pangrova.parser.ast.ExpressionItem;
import org.pangrova.parser.ast.GroupByExpression;
import org.pangrova.parser.ast.GroupByItem;
import org.pangrova.parser.ast.GroupingSets;
import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.JoinClause;
import org.pangrova.parser.ast.PivotClause;
import org.pangrova.parser.ast.Query;
import org.pangrova.parser.ast.RollupGroupBy;
import org.pangrova.parser.ast.SelectItem;
import org.pangrova.parser.ast.SetQuery;
import org.pangrova.parser.ast.SortItem;
import org.pangrova.parser.ast.Statement;
import org.pangrova.parser.ast.TableFunctionRef;
import org.pangrova.parser.ast.TableRef;
import org.pangrova.parser.ast.TimeTravelSpec;
import org.pangrova.parser.ast.UnpivotClause;
import org.apache.calcite.sql.JoinConditionType;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlPivot;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlSelectKeyword;
import org.apache.calcite.sql.SqlSnapshot;
import org.apache.calcite.sql.SqlUnpivot;
import org.apache.calcite.sql.SqlUnresolvedFunction;
import org.apache.calcite.sql.SqlWith;
import org.apache.calcite.sql.SqlWithItem;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.ArrayList;
import java.util.List;

/**
 * Converts Pangrova AST nodes to Calcite SqlNode tree.
 * Expression conversion is delegated to {@link ExpressionToSqlNodeConverter}.
 * Supports: SELECT (with DISTINCT), JOIN, UNION ALL, IN, window functions,
 * subqueries, and custom Pangrova extensions.
 *
 * <p>Extension statement conversion is delegated to
 * {@link ExtensionStatementSqlNodeConverter}.</p>
 */
public final class AstToSqlNodeConverter {

    private static final SqlParserPos POS = SqlParserPos.ZERO;
    private final ExpressionToSqlNodeConverter exprConverter = new ExpressionToSqlNodeConverter(this);
    private final ExtensionStatementSqlNodeConverter extensionConverter =
            new ExtensionStatementSqlNodeConverter(this);

    /**
     * Converts an Pangrova AST statement to a Calcite SqlNode tree.
     *
     * @param stmt statement to convert
     * @return equivalent Calcite SqlNode
     */
    public SqlNode convert(Statement stmt) {
        if (stmt instanceof CteQuery cte) return convertCteQuery(cte);
        if (stmt instanceof SetQuery sq) return convertSetQuery(sq);
        if (stmt instanceof Query q) return convertQuery(q);
        return extensionConverter.convert(stmt);
    }

    public SqlNode convertExpression(Expression expr) {
        return exprConverter.convert(expr);
    }

    // ── CTE query (WITH ... AS ...) ──

    private SqlNode convertCteQuery(CteQuery cte) {
        List<SqlNode> withItems = cte.ctes().stream().map(def -> {
            SqlIdentifier name = new SqlIdentifier(def.name(), POS);
            SqlNode query = convert(def.query());
            return (SqlNode) new SqlWithItem(POS, name, null, query);
        }).toList();
        SqlNodeList withList = new SqlNodeList(withItems, POS);
        return new SqlWith(POS, withList, convert(cte.body()));
    }

    // ── Set query (UNION ALL / UNION / INTERSECT / EXCEPT) ──

    private SqlNode convertSetQuery(SetQuery sq) {
        List<Query> queries = sq.queries();
        List<SetQuery.SetOperatorType> operators = sq.operators();
        SqlNode result = convertQuery(queries.get(0));
        for (int i = 1; i < queries.size(); i++) {
            SqlOperator op = mapSetOperator(operators.get(i - 1));
            result = new SqlBasicCall(op, new SqlNode[]{result, convertQuery(queries.get(i))}, POS);
        }
        return result;
    }

    private static SqlOperator mapSetOperator(SetQuery.SetOperatorType type) {
        return switch (type) {
            case UNION_ALL -> SqlStdOperatorTable.UNION_ALL;
            case UNION_DISTINCT -> SqlStdOperatorTable.UNION;
            case INTERSECT -> SqlStdOperatorTable.INTERSECT;
            case EXCEPT -> SqlStdOperatorTable.EXCEPT;
        };
    }

    // ── Query ──

    SqlNode convertQuery(Query query) {
        SqlNodeList keywords = query.distinct()
                ? new SqlNodeList(List.of(SqlSelectKeyword.DISTINCT.symbol(POS)), POS) : null;
        SqlNodeList selectList = convertSelectItems(query.selectItems());
        SqlNode from = convertFrom(query.from(), query.joins());
        SqlNode where = query.where().map(this::convertExpression).orElse(null);
        SqlNodeList groupBy = convertGroupBy(query.groupBy());
        SqlNode having = query.having().map(this::convertExpression).orElse(null);
        SqlNodeList orderBy = convertOrderBy(query.orderBy());
        SqlNode fetch = query.limit().map(this::convertExpression).orElse(null);
        SqlNode offset = query.offset().map(this::convertExpression).orElse(null);

        if (query.qualify().isEmpty()) {
            return new SqlSelect(POS, keywords, selectList, from, where,
                    groupBy, having, null, orderBy, offset, fetch, null);
        }

        // QUALIFY rewrite: wrap query in a subquery that computes the qualify
        // predicate as a hidden column, then filter on it in an outer query.
        // Original:  SELECT <cols> FROM t WHERE ... QUALIFY <pred>
        // Rewrite:   SELECT <cols> FROM (
        //              SELECT *, <pred> AS __qualify__ FROM t WHERE ...
        //            ) __qsub WHERE __qualify__
        String qualifyAlias = "__qualify__";
        SqlNode qualifyExpr = convertExpression(query.qualify().get());
        SqlNode qualifyAliased = SqlStdOperatorTable.AS.createCall(POS, qualifyExpr,
                new SqlIdentifier(qualifyAlias, POS));

        // Inner select: *, <qualify_pred> AS __qualify__
        List<SqlNode> innerItems = new ArrayList<>();
        innerItems.add(SqlIdentifier.star(POS));
        innerItems.add(qualifyAliased);
        SqlNodeList innerSelectList = new SqlNodeList(innerItems, POS);

        SqlSelect inner = new SqlSelect(POS, keywords, innerSelectList, from, where,
                groupBy, having, null, null, null, null, null);

        // Outer: SELECT <original_cols> FROM (inner) __qsub WHERE __qualify__
        SqlNode outerFrom = SqlStdOperatorTable.AS.createCall(POS, inner,
                new SqlIdentifier("__qsub", POS));
        SqlNode outerWhere = new SqlIdentifier(qualifyAlias, POS);
        return new SqlSelect(POS, null, selectList, outerFrom, outerWhere,
                null, null, null, orderBy, offset, fetch, null);
    }

    // ── Select items ──

    private SqlNodeList convertSelectItems(List<SelectItem> items) {
        return new SqlNodeList(items.stream().map(this::convertSelectItem).toList(), POS);
    }

    private SqlNode convertSelectItem(SelectItem item) {
        if (item instanceof AllColumns) return SqlIdentifier.star(POS);
        ExpressionItem ei = (ExpressionItem) item;
        SqlNode expr = convertExpression(ei.expression());
        if (ei.alias().isPresent()) {
            return SqlStdOperatorTable.AS.createCall(POS, expr, new SqlIdentifier(ei.alias().get(), POS));
        }
        return expr;
    }

    // ── FROM clause ──

    private SqlNode convertFrom(List<TableRef> tables, List<JoinClause> joins) {
        if (tables.isEmpty()) return null;
        SqlNode result = convertTableRef(tables.get(0));
        for (int i = 1; i < tables.size(); i++) {
            result = new SqlJoin(POS, result,
                    SqlLiteral.createBoolean(false, POS),
                    JoinType.COMMA.symbol(POS),
                    convertTableRef(tables.get(i)),
                    JoinConditionType.NONE.symbol(POS), null);
        }
        for (JoinClause join : joins) {
            result = convertJoinClause(result, join);
        }
        return result;
    }

    private SqlNode convertJoinClause(SqlNode left, JoinClause join) {
        JoinType calciteJoinType = mapJoinType(join.joinType());
        SqlNode right = convertTableRef(join.right());
        SqlNode condition = join.condition().map(this::convertExpression).orElse(null);
        JoinConditionType condType = condition != null ? JoinConditionType.ON : JoinConditionType.NONE;
        return new SqlJoin(POS, left, SqlLiteral.createBoolean(false, POS),
                calciteJoinType.symbol(POS), right, condType.symbol(POS), condition);
    }

    private static JoinType mapJoinType(JoinClause.JoinType jt) {
        return switch (jt) {
            case INNER -> JoinType.INNER;
            case LEFT -> JoinType.LEFT;
            case RIGHT -> JoinType.RIGHT;
            case FULL -> JoinType.FULL;
            case CROSS -> JoinType.CROSS;
            case COMMA -> JoinType.COMMA;
        };
    }

    private SqlNode convertTableRef(TableRef ref) {
        if (ref.isValues()) {
            var valuesTable = ref.valuesTable().orElseThrow();
            SqlNode[] rows = valuesTable.rows().stream()
                    .map(row -> SqlStdOperatorTable.ROW.createCall(POS,
                            row.stream().map(this::convertExpression).toArray(SqlNode[]::new)))
                    .toArray(SqlNode[]::new);
            SqlNode values = SqlStdOperatorTable.VALUES.createCall(POS, rows);
            if (ref.alias().isEmpty()) {
                return values;
            }
            List<SqlNode> operands = new ArrayList<>();
            operands.add(values);
            operands.add(new SqlIdentifier(ref.alias().orElseThrow(), POS));
            valuesTable.columnAliases().stream()
                    .map(name -> new SqlIdentifier(name, POS)).forEach(operands::add);
            return SqlStdOperatorTable.AS.createCall(POS, operands.toArray(SqlNode[]::new));
        }
        if (ref.isTableFunction()) {
            return convertLateralTableFunction(ref);
        }
        if (ref.isDerived()) {
            SqlNode subquery = convert(ref.subquery().orElseThrow());
            if (ref.lateral()) {
                subquery = SqlStdOperatorTable.LATERAL.createCall(POS, subquery);
            }
            return wrapAlias(subquery, ref.alias());
        }
        Identifier tableName = ref.systemTableRef().map(systemTable -> {
            String kind = systemTable.systemTableName();
            if (!kind.equalsIgnoreCase("snapshots") && !kind.equalsIgnoreCase("history")
                    && !kind.equalsIgnoreCase("tags") && !kind.equalsIgnoreCase("options")
                    && !kind.equalsIgnoreCase("schemas")
                    && !kind.equalsIgnoreCase("branches")
                    && !kind.equalsIgnoreCase("manifests")
                    && !kind.equalsIgnoreCase("files")
                    && !kind.equalsIgnoreCase("entries")) {
                throw new PlannerException(PangrovaErrorCode.PLANNER_VALIDATION_FAILURE,
                        "System table has no Native reader: " + kind);
            }
            List<String> parts = new ArrayList<>(systemTable.baseTable().parts());
            int last = parts.size() - 1;
            parts.set(last, parts.get(last) + "$" + kind);
            return new Identifier(parts);
        }).orElse(ref.name());
        SqlNode node = applyTimeTravel(id(tableName), ref);
        // UNPIVOT: wrap table in SqlUnpivot
        if (ref.unpivotClause().isPresent()) {
            node = convertUnpivot(node, ref.unpivotClause().get());
        }
        // PIVOT: wrap table in SqlPivot
        if (ref.pivotClause().isPresent()) {
            node = convertPivot(node, ref.pivotClause().get());
        }
        return wrapAlias(node, ref.alias());
    }

    private SqlNode convertLateralTableFunction(TableRef ref) {
        var funcRef = ref.tableFunctionRef().orElseThrow();
        if (funcRef.functionType() == TableFunctionRef.TableFunctionType.GENERATE_SERIES) {
            return convertGenerateSeries(funcRef);
        }
        SqlNode argument = convertExpression(funcRef.argument());
        SqlNode unnest;
        if (funcRef.functionType() == TableFunctionRef.TableFunctionType.POSEXPLODE
                || funcRef.functionType() == TableFunctionRef.TableFunctionType.UNNEST_WITH_ORDINALITY) {
            // Preserve the value column followed by the ordinality column.
            unnest = new org.apache.calcite.sql.SqlUnnestOperator(true)
                    .createCall(POS, argument);
        } else {
            unnest = SqlStdOperatorTable.UNNEST.createCall(POS, argument);
        }
        if (funcRef.alias().isPresent()) {
            List<SqlNode> operands = new java.util.ArrayList<>();
            operands.add(unnest);
            operands.add(new SqlIdentifier(funcRef.alias().get(), POS));
            for (String colAlias : funcRef.columnAliases()) {
                operands.add(new SqlIdentifier(colAlias, POS));
            }
            return SqlStdOperatorTable.AS.createCall(POS, operands.toArray(SqlNode[]::new));
        }
        return unnest;
    }

    private SqlNode convertGenerateSeries(TableFunctionRef funcRef) {
        List<SqlNode> arguments = new ArrayList<>();
        arguments.add(convertExpression(funcRef.argument()));
        arguments.add(convertExpression(funcRef.endArgument().orElseThrow(
                () -> new IllegalArgumentException("GENERATE_SERIES requires start and stop"))));
        funcRef.stepArgument().ifPresent(step -> arguments.add(convertExpression(step)));
        SqlNode invocation = new SqlUnresolvedFunction(new SqlIdentifier("GENERATE_SERIES", POS),
                null, null, null, null, SqlFunctionCategory.USER_DEFINED_TABLE_FUNCTION)
                .createCall(POS, arguments.toArray(SqlNode[]::new));
        SqlNode table = SqlStdOperatorTable.COLLECTION_TABLE.createCall(POS, invocation);
        List<SqlNode> alias = new ArrayList<>();
        alias.add(table);
        alias.add(new SqlIdentifier(funcRef.alias().orElse("generate_series"), POS));
        funcRef.columnAliases().stream().map(name -> new SqlIdentifier(name, POS))
                .forEach(alias::add);
        return SqlStdOperatorTable.AS.createCall(POS, alias.toArray(SqlNode[]::new));
    }

    private SqlNode applyTimeTravel(SqlNode table, TableRef ref) {
        if (ref.timeTravelSpec().isEmpty()) {
            return table;
        }
        TimeTravelSpec spec = ref.timeTravelSpec().get();
        SqlNode period = switch (spec.type()) {
            case BRANCH -> SqlLiteral.createCharString("branch:" + spec.refName(), POS);
            case TAG -> SqlLiteral.createCharString("tag:" + spec.refName(), POS);
            case SYSTEM_TIME, SYSTEM_VERSION -> convertExpression(spec.expression());
        };
        return new SqlSnapshot(POS, table, period);
    }

    private static SqlNode wrapAlias(SqlNode node, java.util.Optional<String> alias) {
        if (alias.isEmpty()) {
            return node;
        }
        return SqlStdOperatorTable.AS.createCall(POS, node, new SqlIdentifier(alias.get(), POS));
    }

    // ── GROUP BY / ORDER BY ──

    private SqlNodeList convertGroupBy(List<GroupByItem> groupBy) {
        if (groupBy.isEmpty()) return null;
        return new SqlNodeList(groupBy.stream().map(this::convertGroupByItem).toList(), POS);
    }

    private SqlNode convertGroupByItem(GroupByItem item) {
        if (item instanceof GroupByExpression gbe) {
            return convertExpression(gbe.expression());
        }
        if (item instanceof GroupingSets gs) {
            List<List<Expression>> sets = gs.sets();
            if (isRollupPattern(sets)) {
                SqlNode[] cols = sets.get(0).stream()
                        .map(this::convertExpression).toArray(SqlNode[]::new);
                return SqlStdOperatorTable.ROLLUP.createCall(POS, cols);
            }
            if (isCubePattern(sets)) {
                SqlNode[] cols = sets.get(0).stream()
                        .map(this::convertExpression).toArray(SqlNode[]::new);
                return SqlStdOperatorTable.CUBE.createCall(POS, cols);
            }
            return SqlStdOperatorTable.GROUPING_SETS.createCall(
                    POS,
                    sets.stream()
                            .map(set -> (SqlNode) new SqlNodeList(
                                    set.stream().map(this::convertExpression).toList(), POS))
                            .toArray(SqlNode[]::new));
        }
        if (item instanceof CubeGroupBy cube) {
            return SqlStdOperatorTable.CUBE.createCall(
                    POS,
                    cube.columns().stream().map(this::convertExpression).toArray(SqlNode[]::new));
        }
        if (item instanceof RollupGroupBy rollup) {
            return SqlStdOperatorTable.ROLLUP.createCall(
                    POS,
                    rollup.columns().stream().map(this::convertExpression).toArray(SqlNode[]::new));
        }
        throw new IllegalArgumentException("Unknown GroupByItem type: " + item.getClass().getSimpleName());
    }

    private boolean isRollupPattern(List<List<Expression>> sets) {
        if (sets.isEmpty() || !sets.get(sets.size() - 1).isEmpty()) return false;
        int maxSize = sets.get(0).size();
        if (maxSize == 0) return false;
        if (sets.size() != maxSize + 1) return false;
        for (int i = 0; i < sets.size(); i++) {
            if (sets.get(i).size() != maxSize - i) return false;
        }
        return true;
    }

    private boolean isCubePattern(List<List<Expression>> sets) {
        if (sets.isEmpty()) return false;
        int n = sets.get(0).size();
        if (n == 0) return false;
        return sets.size() == (1 << n) && sets.stream().anyMatch(List::isEmpty);
    }

    SqlNodeList convertOrderBy(List<SortItem> orderBy) {
        if (orderBy.isEmpty()) return null;
        return new SqlNodeList(orderBy.stream().map(this::convertSortItem).toList(), POS);
    }

    private SqlNode convertSortItem(SortItem item) {
        SqlNode expr = convertExpression(item.expression());
        if (item.direction().isPresent() && item.direction().get() == SortItem.SortDirection.DESC) {
            expr = SqlStdOperatorTable.DESC.createCall(POS, expr);
        }
        if (item.nullOrdering().isPresent()) {
            if (item.nullOrdering().get() == SortItem.NullOrdering.FIRST) {
                expr = SqlStdOperatorTable.NULLS_FIRST.createCall(POS, expr);
            } else {
                expr = SqlStdOperatorTable.NULLS_LAST.createCall(POS, expr);
            }
        }
        return expr;
    }

    // ── Helpers ──

    private SqlNode id(Identifier ident) { return new SqlIdentifier(ident.parts(), POS); }

    // ── UNPIVOT / PIVOT ──

    private SqlNode convertUnpivot(SqlNode source, UnpivotClause unpivot) {
        boolean includeNulls = unpivot.nullHandling() == UnpivotClause.NullHandling.INCLUDE;
        // measureList: the value column(s)
        SqlNodeList measureList = new SqlNodeList(
                List.of(new SqlIdentifier(unpivot.valueColumn().parts(), POS)), POS);
        // axisList: the FOR column(s)
        SqlNodeList axisList = new SqlNodeList(
                List.of(new SqlIdentifier(unpivot.nameColumn().parts(), POS)), POS);
        // inList: each source column as a SqlNodeList entry
        List<SqlNode> inItems = unpivot.sourceColumns().stream().map(col -> {
            SqlIdentifier colId = new SqlIdentifier(col.column().parts(), POS);
            if (col.alias().isPresent()) {
                SqlNode alias = SqlLiteral.createCharString(col.alias().get(), POS);
                return (SqlNode) new SqlNodeList(List.of(colId, alias), POS);
            }
            return (SqlNode) new SqlNodeList(List.of(colId), POS);
        }).toList();
        SqlNodeList inList = new SqlNodeList(inItems, POS);
        return new SqlUnpivot(POS, source, includeNulls, measureList, axisList, inList);
    }

    private SqlNode convertPivot(SqlNode source, PivotClause pivot) {
        // aggList: aggregate function calls
        List<SqlNode> aggItems = pivot.aggregates().stream().map(agg -> {
            SqlNode arg = convertExpression(agg.argument());
            SqlNode call = new SqlBasicCall(
                    new SqlUnresolvedFunction(new SqlIdentifier(agg.function().parts(), POS),
                            null, null, null, null, SqlFunctionCategory.USER_DEFINED_FUNCTION),
                    new SqlNode[]{arg}, POS);
            if (agg.alias().isPresent()) {
                return (SqlNode) SqlStdOperatorTable.AS.createCall(POS, call,
                        new SqlIdentifier(agg.alias().get(), POS));
            }
            return call;
        }).toList();
        SqlNodeList aggList = new SqlNodeList(aggItems, POS);
        // axisList: FOR column
        SqlNodeList axisList = new SqlNodeList(
                List.of(new SqlIdentifier(pivot.forColumn().parts(), POS)), POS);
        // inList: pivot values
        List<SqlNode> inItems = pivot.inValues().stream().map(pv -> {
            SqlNode val = convertExpression(pv.value());
            if (pv.alias().isPresent()) {
                return (SqlNode) SqlStdOperatorTable.AS.createCall(POS, val,
                        new SqlIdentifier(pv.alias().get(), POS));
            }
            return val;
        }).toList();
        SqlNodeList inList = new SqlNodeList(inItems, POS);
        return new SqlPivot(POS, source, aggList, axisList, inList);
    }
}

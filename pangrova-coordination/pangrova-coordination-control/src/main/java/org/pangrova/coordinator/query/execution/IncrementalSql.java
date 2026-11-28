package org.pangrova.coordinator.query.execution;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.connector.api.metadata.table.SnapshotRange;
import org.pangrova.parser.api.ast.AllColumns;
import org.pangrova.parser.api.ast.Expression;
import org.pangrova.parser.api.ast.ExpressionItem;
import org.pangrova.parser.api.ast.Identifier;
import org.pangrova.parser.api.ast.Literal;
import org.pangrova.parser.api.ast.LiteralValue;
import org.pangrova.parser.api.ast.Query;
import org.pangrova.parser.api.ast.TableRef;
import org.pangrova.parser.api.ast.statement.IncrementalReadStatement;
import org.pangrova.parser.syntax.TimeTravelSqlClauses.Analysis;
import org.pangrova.planner.api.exception.PlannerException;
import org.pangrova.planner.metadata.catalog.PlanningSnapshotSelectors;
import org.pangrova.planner.rewrite.DynamicTableSqlRewriter;
import org.pangrova.query.api.session.QuerySession;
import org.pangrova.query.session.BranchRoutingContext;
import org.pangrova.query.session.SessionTableNames;

/** Lowers an append-range statement to ordinary relational SQL without parsing a second time. */
record IncrementalSql(Analysis analysis, QuerySession session) {
    static IncrementalSql bind(Analysis input, IncrementalReadStatement changes, QuerySession session) {
        var table = SessionTableNames.resolveTableRef(changes.tableName(), session);
        var range = new SnapshotRange(endpoint(changes.fromSnapshot()), endpoint(changes.toSnapshot()));
        String key = PlanningSnapshotSelectors.tableKey(table.catalog(), table.schema(), table.table());
        var parts = List.of(table.catalog(), table.schema(), table.table());
        DynamicTableSqlRewriter.requireOrdinaryPhysicalName(parts);
        if (BranchRoutingContext.fromSessionProperties(session.sessionProperties()).getBranch(key).isPresent()) {
            throw new PlannerException(PangrovaErrorCode.PLANNER_VALIDATION_FAILURE,
                    "CHANGES requires a table without session branch routing");
        }
        var base = new TableRef(new Identifier(parts));
        var rows = new Query(false,
                List.of(new AllColumns(), new ExpressionItem(Literal.ofString("INSERT"), Optional.of("_change_type"))),
                List.of(base), Optional.empty(), List.of(), Optional.empty(), List.of(), Optional.empty());
        var query = new Query(false, changes.selectItems(),
                List.of(TableRef.derived(rows, Optional.of(table.table()))),
                changes.where(), List.of(), Optional.empty(), changes.orderBy(), changes.limit());
        // The source is a physical relation selected by the bound range, so textual view expansion
        // must not replace it. Keep the submitted SQL for audit and admission identity.
        return new IncrementalSql(new Analysis(input.sql(), query, input.sql(), List.of()),
                session.withSourceIncrementalRanges(Map.of(key, range)));
    }

    private static long endpoint(Expression expression) {
        if (expression instanceof Literal literal && literal.value() instanceof LiteralValue.IntegerValue integer
                && integer.value() > 0) {
            return integer.value();
        }
        throw new PlannerException(PangrovaErrorCode.PLANNER_VALIDATION_FAILURE,
                "CHANGES snapshot endpoints must be positive integer literals");
    }
}

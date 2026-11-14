package org.pangrova.planner.branch;

import org.pangrova.parser.api.ast.CteQuery;
import org.pangrova.parser.api.ast.Identifier;
import org.pangrova.parser.api.ast.JoinClause;
import org.pangrova.parser.api.ast.Query;
import org.pangrova.parser.api.ast.SetQuery;
import org.pangrova.parser.api.ast.Statement;
import org.pangrova.parser.api.ast.TableRef;
import org.pangrova.parser.api.ast.TimeTravelSpec;
import org.pangrova.parser.api.ast.statement.dml.InsertSelectStatement;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.pangrova.common.util.Preconditions.checkNotNull;

/**
 * Planner rule that resolves table references to branch versions based on
 * active session branch routing.
 *
 * <p>When USE BRANCH is active for a table, this rule injects a
 * {@link TimeTravelSpec#branch(String)} into the table reference so that
 * downstream planning resolves the table to the branch snapshot.</p>
 *
 * <p>Resolution rules:
 * <ul>
 *   <li>If a table already has an explicit VERSION AS OF BRANCH/TAG or any
 *       time travel spec, the explicit spec takes precedence (no override).</li>
 *   <li>DML source queries (INSERT SELECT) are also resolved.</li>
 *   <li>Table name matching is case-insensitive and supports partial qualification
 *       (e.g., routing for "catalog.schema.table" matches "schema.table" or "table").</li>
 * </ul>
 */
public final class BranchResolutionRule {

    private final BranchRoutingContext routingContext;

    public BranchResolutionRule(BranchRoutingContext routingContext) {
        this.routingContext = checkNotNull(routingContext, "routingContext");
    }

    /**
     * Rewrites a statement by injecting branch time travel specs into table
     * references that have active branch routing.
     *
     * @param statement the parsed AST statement
     * @return rewritten statement with branch specs injected, or the original if no routing applies
     */
    public Statement resolve(Statement statement) {
        if (!routingContext.hasActiveRoutings()) {
            return statement;
        }
        if (statement instanceof Query query) {
            return resolveQuery(query);
        } else if (statement instanceof CteQuery cte) {
            return resolveCteQuery(cte);
        } else if (statement instanceof SetQuery setQuery) {
            return resolveSetQuery(setQuery);
        } else if (statement instanceof InsertSelectStatement insert) {
            return resolveInsertSelect(insert);
        }
        return statement;
    }

    private Query resolveQuery(Query query) {
        List<TableRef> resolvedFrom = resolveTableRefs(query.from());
        List<JoinClause> resolvedJoins = resolveJoinClauses(query.joins());
        if (resolvedFrom.equals(query.from()) && resolvedJoins.equals(query.joins())) {
            return query;
        }
        return new Query(
                query.distinct(),
                query.selectItems(),
                resolvedFrom,
                resolvedJoins,
                query.where(),
                query.groupBy(),
                query.having(),
                query.qualify(),
                query.orderBy(),
                query.rerankBy(),
                query.limit(),
                query.offset());
    }

    private CteQuery resolveCteQuery(CteQuery cte) {
        Statement resolvedBody = resolve(cte.body());
        if (resolvedBody == cte.body()) {
            return cte;
        }
        return new CteQuery(cte.ctes(), resolvedBody);
    }

    private SetQuery resolveSetQuery(SetQuery setQuery) {
        List<Query> resolvedQueries = new ArrayList<>(setQuery.queries().size());
        boolean changed = false;
        for (Query q : setQuery.queries()) {
            Query resolved = resolveQuery(q);
            resolvedQueries.add(resolved);
            if (resolved != q) {
                changed = true;
            }
        }
        return changed ? new SetQuery(resolvedQueries, setQuery.operators()) : setQuery;
    }

    private InsertSelectStatement resolveInsertSelect(InsertSelectStatement insert) {
        SetQuery resolvedSource = resolveSetQuery(insert.selectQuery());
        if (resolvedSource == insert.selectQuery()) {
            return insert;
        }
        return new InsertSelectStatement(
                insert.tableName(),
                insert.columns(),
                insert.overwrite(),
                resolvedSource);
    }

    private List<TableRef> resolveTableRefs(List<TableRef> refs) {
        List<TableRef> resolved = new ArrayList<>(refs.size());
        boolean changed = false;
        for (TableRef ref : refs) {
            TableRef resolvedRef = resolveTableRef(ref);
            resolved.add(resolvedRef);
            if (resolvedRef != ref) {
                changed = true;
            }
        }
        return changed ? List.copyOf(resolved) : refs;
    }

    private List<JoinClause> resolveJoinClauses(List<JoinClause> joins) {
        List<JoinClause> resolved = new ArrayList<>(joins.size());
        boolean changed = false;
        for (JoinClause join : joins) {
            TableRef resolvedRight = resolveTableRef(join.right());
            if (resolvedRight != join.right()) {
                resolved.add(new JoinClause(join.joinType(), resolvedRight, join.condition()));
                changed = true;
            } else {
                resolved.add(join);
            }
        }
        return changed ? List.copyOf(resolved) : joins;
    }

    private TableRef resolveTableRef(TableRef ref) {
        if (ref.isDerived() || ref.isTableFunction()) {
            return ref;
        }
        if (ref.timeTravelSpec().isPresent()) {
            return ref;
        }
        Optional<String> branch = findBranchForTable(ref.name());
        if (branch.isEmpty()) {
            return ref;
        }
        return new TableRef(
                ref.name(),
                Optional.of(TimeTravelSpec.branch(branch.get())),
                ref.alias(),
                ref.subquery(),
                ref.lateral(),
                ref.tableFunctionRef(),
                ref.systemTableRef(),
                ref.tableSample());
    }

    /**
     * Finds the active branch for a table by matching the table identifier
     * against routing entries. Supports partial qualification matching:
     * a routing for "catalog.schema.table" matches references to
     * "schema.table" or just "table".
     */
    private Optional<String> findBranchForTable(Identifier tableName) {
        String fqn = String.join(".", tableName.parts()).toLowerCase();
        Optional<String> exact = routingContext.getRouting(fqn);
        if (exact.isPresent()) {
            return exact;
        }
        for (var entry : routingContext.snapshot().entrySet()) {
            if (entry.getKey().endsWith("." + fqn) || fqn.endsWith("." + entry.getKey())) {
                return Optional.of(entry.getValue());
            }
        }
        return Optional.empty();
    }
}

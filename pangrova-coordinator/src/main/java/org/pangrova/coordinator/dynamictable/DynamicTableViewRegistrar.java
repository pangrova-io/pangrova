package org.pangrova.coordinator.dynamictable;

import org.pangrova.parser.ast.ExpressionItem;
import org.pangrova.parser.ast.GroupByExpression;
import org.pangrova.parser.ast.GroupByItem;
import org.pangrova.parser.ast.Query;
import org.pangrova.parser.ast.RefreshStrategy;
import org.pangrova.parser.ast.SelectItem;
import org.pangrova.planner.incremental.IncrementalAnalysis;
import org.pangrova.planner.incremental.IncrementalAnalyzer;
import org.pangrova.planner.mv.MaterializedViewDefinition;
import org.pangrova.planner.mv.MaterializedViewRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.LinkedHashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Registers and updates the materialized view definitions backing Dynamic Tables,
 * deriving source tables, output columns and group-by columns from the defining query.
 */
final class DynamicTableViewRegistrar {

    private static final Logger LOG = LoggerFactory.getLogger(DynamicTableViewRegistrar.class);

    private final MaterializedViewRegistry registry;

    DynamicTableViewRegistrar(MaterializedViewRegistry registry) {
        this.registry = registry;
    }

    /** Registers a new incremental materialized view definition for the given Dynamic Table. */
    void registerMaterializedView(String dtName, String queryText, Query query) {
        IncrementalAnalysis analysis = IncrementalAnalyzer.analyze(queryText);
        Set<String> tableNames = extractSourceTables(query);
        Set<String> columnNames = extractOutputColumns(query);
        Set<String> groupByColumns = extractGroupByColumns(query);
        MaterializedViewDefinition materializedViewDef = new MaterializedViewDefinition(
                dtName, queryText, RefreshStrategy.INCREMENTAL,
                false, Optional.empty(), Instant.now(), tableNames, columnNames, groupByColumns);
        registry.register(materializedViewDef);
        LOG.info("event=dt_mv_registered dtName={} tables={} columns={} groupBy={} incrementalizable={} strategies={}",
                dtName, tableNames.size(), columnNames.size(), groupByColumns.size(),
                analysis.isIncrementalizable(), analysis.mergeStrategies().size());
    }

    /** Updates the materialized view definition of a redefined Dynamic Table, if present. */
    void updateMaterializedViewQuery(String dtName, String newQueryText, Query newQuery) {
        Optional<MaterializedViewDefinition> existing = registry.getDefinition(dtName);
        if (existing.isPresent()) {
            MaterializedViewDefinition old = existing.get();
            Set<String> tableNames = extractSourceTables(newQuery);
            Set<String> columnNames = extractOutputColumns(newQuery);
            Set<String> groupByColumns = extractGroupByColumns(newQuery);
            MaterializedViewDefinition updated = new MaterializedViewDefinition(
                    old.name(), newQueryText, old.refreshStrategy(),
                    old.enableQueryRewrite(), old.lastRefreshSnapshot(), old.createdAt(),
                    tableNames, columnNames, groupByColumns);
            registry.register(updated);
        }
    }

    /** Extracts the fully-qualified source table names referenced by the query. */
    static Set<String> extractSourceTables(Query query) {
        return query.from().stream()
                .map(ref -> String.join(".", ref.name().parts()))
                .collect(Collectors.toSet());
    }

    private static Set<String> extractOutputColumns(Query query) {
        Set<String> columns = new LinkedHashSet<>();
        for (SelectItem item : query.selectItems()) {
            if (item instanceof ExpressionItem ei) {
                ei.alias().ifPresentOrElse(
                        alias -> columns.add(alias.toLowerCase()),
                        () -> columns.add(ei.expression().toString().toLowerCase()));
            }
        }
        return columns;
    }

    private static Set<String> extractGroupByColumns(Query query) {
        Set<String> columns = new LinkedHashSet<>();
        for (GroupByItem item : query.groupBy()) {
            if (item instanceof GroupByExpression gbe) {
                columns.add(gbe.expression().toString().toLowerCase());
            }
        }
        return columns;
    }
}

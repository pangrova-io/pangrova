package org.pangrova.parser.ast;

import java.util.List;
import java.util.Optional;

/**
 * Represents a table reference with optional time travel, alias, table sample,
 * PIVOT/UNPIVOT clauses, and derived table subquery.
 */
public record TableRef(
        Identifier name,
        Optional<TimeTravelSpec> timeTravelSpec,
        Optional<String> alias,
        Optional<Statement> subquery,
        boolean lateral,
        Optional<TableFunctionRef> tableFunctionRef,
        Optional<SystemTableReference> systemTableRef,
        Optional<TableSample> tableSample,
        Optional<PivotClause> pivotClause,
        Optional<UnpivotClause> unpivotClause) {

    public TableRef(Identifier name) {
        this(name, Optional.empty(), Optional.empty(), Optional.empty(), false,
                Optional.empty(), Optional.empty(), Optional.empty(),
                Optional.empty(), Optional.empty());
    }

    public TableRef(Identifier name, Optional<TimeTravelSpec> timeTravelSpec, Optional<String> alias) {
        this(name, timeTravelSpec, alias, Optional.empty(), false,
                Optional.empty(), Optional.empty(), Optional.empty(),
                Optional.empty(), Optional.empty());
    }

    /** Backward-compatible constructor without PIVOT/UNPIVOT fields. */
    public TableRef(Identifier name,
                    Optional<TimeTravelSpec> timeTravelSpec,
                    Optional<String> alias,
                    Optional<Statement> subquery,
                    boolean lateral,
                    Optional<TableFunctionRef> tableFunctionRef,
                    Optional<SystemTableReference> systemTableRef,
                    Optional<TableSample> tableSample) {
        this(name, timeTravelSpec, alias, subquery, lateral, tableFunctionRef,
                systemTableRef, tableSample, Optional.empty(), Optional.empty());
    }

    /** Creates a named table reference with TABLESAMPLE clause. */
    public static TableRef sampled(Identifier name,
                                   Optional<TimeTravelSpec> timeTravelSpec,
                                   Optional<String> alias,
                                   TableSample tableSample) {
        return new TableRef(name, timeTravelSpec, alias, Optional.empty(), false,
                Optional.empty(), Optional.empty(), Optional.of(tableSample),
                Optional.empty(), Optional.empty());
    }

    /** Creates a system table reference (e.g., {@code catalog.schema.table$snapshots}). */
    public static TableRef systemTable(SystemTableReference sysRef,
                                        Optional<TimeTravelSpec> timeTravelSpec,
                                        Optional<String> alias) {
        return new TableRef(sysRef.baseTable(), timeTravelSpec, alias, Optional.empty(), false,
                Optional.empty(), Optional.of(sysRef), Optional.empty(),
                Optional.empty(), Optional.empty());
    }

    /** Creates a derived table reference (subquery in FROM clause). */
    public static TableRef derived(Statement subquery, Optional<String> alias) {
        return new TableRef(new Identifier(List.of("__derived__")), Optional.empty(), alias,
                Optional.of(subquery), false, Optional.empty(), Optional.empty(), Optional.empty(),
                Optional.empty(), Optional.empty());
    }

    /** Creates a LATERAL derived table reference. */
    public static TableRef lateralDerived(Statement subquery, Optional<String> alias) {
        return new TableRef(new Identifier(List.of("__lateral_derived__")), Optional.empty(), alias,
                Optional.of(subquery), true, Optional.empty(), Optional.empty(), Optional.empty(),
                Optional.empty(), Optional.empty());
    }

    /** Creates a LATERAL TABLE(EXPLODE/POSEXPLODE) reference. */
    public static TableRef lateralTableFunction(TableFunctionRef funcRef) {
        return new TableRef(new Identifier(List.of("__lateral_func__")), Optional.empty(),
                funcRef.alias(), Optional.empty(), true, Optional.of(funcRef), Optional.empty(),
                Optional.empty(), Optional.empty(), Optional.empty());
    }

    /** Creates a PIVOT table reference wrapping a source table. */
    public static TableRef pivot(TableRef source, PivotClause pivot, Optional<String> alias) {
        return new TableRef(source.name(), source.timeTravelSpec(), alias,
                source.subquery(), source.lateral(), source.tableFunctionRef(),
                source.systemTableRef(), source.tableSample(),
                Optional.of(pivot), Optional.empty());
    }

    /** Creates an UNPIVOT table reference wrapping a source table. */
    public static TableRef unpivot(TableRef source, UnpivotClause unpivot, Optional<String> alias) {
        return new TableRef(source.name(), source.timeTravelSpec(), alias,
                source.subquery(), source.lateral(), source.tableFunctionRef(),
                source.systemTableRef(), source.tableSample(),
                Optional.empty(), Optional.of(unpivot));
    }

    /** Returns the structural kind of this table reference. */
    public TableRefKind kind() {
        if (pivotClause.isPresent()) return TableRefKind.PIVOT;
        if (unpivotClause.isPresent()) return TableRefKind.UNPIVOT;
        if (systemTableRef.isPresent()) return TableRefKind.SYSTEM_TABLE;
        if (tableFunctionRef.isPresent()) return TableRefKind.LATERAL_FUNCTION;
        if (subquery.isPresent() && lateral) return TableRefKind.LATERAL_DERIVED;
        if (subquery.isPresent()) return TableRefKind.DERIVED;
        return TableRefKind.NAMED;
    }

    /** Returns true if this is a derived table (subquery in FROM). */
    public boolean isDerived() {
        return subquery.isPresent();
    }

    /** Returns true if this is a LATERAL TABLE function reference. */
    public boolean isTableFunction() {
        return tableFunctionRef.isPresent();
    }

    /** Returns true if this is a system table reference (e.g., {@code table$snapshots}). */
    public boolean isSystemTable() {
        return systemTableRef.isPresent();
    }
}

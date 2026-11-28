package org.pangrova.coordinator.query.execution.dispatch.listener;

import java.util.function.Consumer;
import java.util.function.Supplier;

import org.pangrova.query.api.enums.StatementCategory;
import org.pangrova.query.dispatch.StatementCompletionEvent;
import org.pangrova.query.dispatch.StatementCompletionListener;
import org.pangrova.planner.analysis.SqlCompiler;

import static org.pangrova.common.validation.Preconditions.checkNotNull;

/**
 * Refreshes the SQL compiler schema after DDL/DML mutations.
 * Priority 10 — runs first so subsequent listeners see updated schema.
 */
public final class SchemaRefreshListener implements StatementCompletionListener {

    private final Supplier<SqlCompiler> compilerRefreshSupplier;
    private final Consumer<SqlCompiler> compilerUpdateCallback;

    public SchemaRefreshListener(
            Supplier<SqlCompiler> compilerRefreshSupplier,
            Consumer<SqlCompiler> compilerUpdateCallback) {
        this.compilerRefreshSupplier = checkNotNull(compilerRefreshSupplier, "compilerRefreshSupplier");
        this.compilerUpdateCallback = checkNotNull(compilerUpdateCallback, "compilerUpdateCallback");
    }

    @Override
    public void onCompleted(StatementCompletionEvent event) {
        StatementCategory category = event.result().category();
        if (isSchemaAffectingCategory(category)) {
            SqlCompiler refreshed = compilerRefreshSupplier.get();
            compilerUpdateCallback.accept(refreshed);
        }
    }

    /**
     * Returns true for categories that affect the SQL compiler's schema model.
     * DDL, DML, dynamic table, index, clustering, and stream operations all
     * modify metadata that the compiler needs to reflect for subsequent queries.
     */
    private static boolean isSchemaAffectingCategory(StatementCategory category) {
        return category == StatementCategory.DDL
                || category == StatementCategory.DML
                || category == StatementCategory.DYNAMIC_TABLE
                || category == StatementCategory.INDEX
                || category == StatementCategory.CLUSTERING
                || category == StatementCategory.STREAM;
    }

    @Override
    public int priority() {
        return 10;
    }
}

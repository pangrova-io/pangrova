package org.pangrova.parser.api.ast.statement;

import java.time.Duration;

import org.pangrova.parser.api.ast.Identifier;
import org.pangrova.parser.api.ast.Query;
import org.pangrova.parser.api.enums.RefreshMode;

/**
 * Actions for {@code ALTER DYNAMIC TABLE} statements.
 */
public sealed interface AlterDynamicTableAction {

    /** Suspend automatic refresh scheduling. */
    record Suspend() implements AlterDynamicTableAction {}

    /** Resume automatic refresh scheduling. */
    record Resume() implements AlterDynamicTableAction {}

    /** Trigger an immediate manual refresh. */
    record Refresh() implements AlterDynamicTableAction {}

    /** Update the target lag duration. */
    record SetTargetLag(Duration newLag) implements AlterDynamicTableAction {}

    /** Change the refresh mode; the kernel follows with a full publication. */
    record SetRefreshMode(RefreshMode mode) implements AlterDynamicTableAction {}

    /** Rename the logical object. */
    record Rename(Identifier newName) implements AlterDynamicTableAction {}

    /** Redefine the Dynamic Table with a new query. */
    record Redefine(Query newQuery) implements AlterDynamicTableAction {}

    /** Backfill embedding for a specific column. */
    record BackfillEmbedding(String columnName) implements AlterDynamicTableAction {}
}

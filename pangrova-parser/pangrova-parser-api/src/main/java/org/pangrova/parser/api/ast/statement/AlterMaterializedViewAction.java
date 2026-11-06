package org.pangrova.parser.api.ast.statement;

import java.time.Duration;

import org.pangrova.parser.api.ast.Identifier;
import org.pangrova.parser.api.ast.RefreshMode;

/**
 * Actions for {@code ALTER MATERIALIZED VIEW} statements.
 */
public sealed interface AlterMaterializedViewAction {

    /** Suspend automatic refresh scheduling. */
    record Suspend() implements AlterMaterializedViewAction {}

    /** Resume automatic refresh scheduling. */
    record Resume() implements AlterMaterializedViewAction {}

    /** Trigger an immediate manual refresh. */
    record Refresh() implements AlterMaterializedViewAction {}

    /** Update the target lag duration. */
    record SetTargetLag(Duration newLag) implements AlterMaterializedViewAction {}

    /** Change the refresh mode; the kernel follows with a full publication. */
    record SetRefreshMode(RefreshMode mode) implements AlterMaterializedViewAction {}

    /** Rename the logical object. */
    record Rename(Identifier newName) implements AlterMaterializedViewAction {}
}

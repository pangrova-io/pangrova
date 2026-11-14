package org.pangrova.parser.api.ast.statement;

import java.util.Optional;

import org.pangrova.parser.api.ast.Identifier;

/**
 * Actions for {@code ALTER STAGE} statements.
 */
public sealed interface AlterStageAction {

    /** Refresh the directory projection, optionally limited to a subpath. */
    record Refresh(Optional<String> subpath) implements AlterStageAction {}

    /** Point an external stage at a different location. */
    record SetUrl(String url) implements AlterStageAction {}

    /** Point the stage at a different storage profile. */
    record SetStorageProfile(String storageProfile) implements AlterStageAction {}

    /** Enable or disable the stage directory projection. */
    record SetDirectoryEnabled(boolean enabled) implements AlterStageAction {}

    /** Replace the stage comment. */
    record SetComment(String comment) implements AlterStageAction {}

    /** Transfer ownership of the stage. */
    record SetOwner(String owner) implements AlterStageAction {}

    /** Rename the logical object. */
    record Rename(Identifier newName) implements AlterStageAction {}
}

package org.pangrova.parser.api.ast.statement;

import org.pangrova.parser.api.ast.Identifier;

/**
 * Actions for {@code ALTER VOLUME} statements.
 */
public sealed interface AlterVolumeAction {

    /** Point the volume at a different storage profile. */
    record SetStorageProfile(String storageProfile) implements AlterVolumeAction {}

    /** Replace the volume comment. */
    record SetComment(String comment) implements AlterVolumeAction {}

    /** Transfer ownership of the volume. */
    record SetOwner(String owner) implements AlterVolumeAction {}

    /** Rename the logical object. */
    record Rename(Identifier newName) implements AlterVolumeAction {}
}

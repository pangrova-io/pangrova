package org.pangrova.file.api.space;

import org.pangrova.file.api.enums.FileSpaceType;

import org.pangrova.common.api.annotation.Experimental;

import java.util.Optional;
import java.util.UUID;

/** Resolves an exact file namespace identity or qualified name. */
@Experimental
public interface FileSpaceLookup {
    /** Looks up an immutable namespace identity; dropped IDs never resolve to replacements. */
    Optional<FileSpace> find(UUID id);

    /** Looks up one space. */
    Optional<FileSpace> find(FileSpaceType type, String catalog, String schema, String name);
}

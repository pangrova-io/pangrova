package org.pangrova.persistence.file.api.space;

import org.pangrova.file.api.space.FileSpace;
import org.pangrova.file.api.space.FileSpaceDefinition;
import org.pangrova.file.api.space.FileSpaceMutation;

import java.util.List;

import org.pangrova.file.api.space.FileSpaceLookup;
import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.file.api.enums.FileSpaceType;

/**
 * Durable registry for Volumes and Stages.
 *
 * <p>Both syntax surfaces share this single registry so that file discovery,
 * authorization, and the future file index see one namespace model. The
 * registry only stores namespace metadata; it never stores file bytes or
 * per-file asset/version rows.</p>
 *
 * <p>Thread-safe: implementations must support concurrent reads and writes.</p>
 */
@Experimental
public interface FileSpaceRegistry extends FileSpaceLookup {

    /**
     * Creates a space. When {@code ifNotExists} is true and the name is already
     * taken, the existing definition is returned unchanged.
     *
     * <p>Implementations reject a duplicate name when {@code ifNotExists} is
     * false, and reject definitions that violate namespace invariants.</p>
     */
    FileSpace create(FileSpaceDefinition definition, boolean ifNotExists);

    /** Applies one mutation and returns the updated definition. */
    FileSpace mutate(FileSpaceType type, String catalog, String schema, String name,
            FileSpaceMutation mutation);

    /** Renames a space without changing its catalog or schema. */
    FileSpace rename(FileSpaceType type, String catalog, String schema, String name,
            String newName);



    /**
     * Lists spaces of one type. A null {@code catalog} lists every space of that
     * type; a non-null catalog with a null schema lists the whole catalog.
     */
    List<FileSpace> list(FileSpaceType type, String catalog, String schema);

    /**
     * Drops a space.
     *
     * <p>Implementations reject a missing space when {@code ifExists} is false.</p>
     *
     * @return true if a row was removed
     */
    boolean drop(FileSpaceType type, String catalog, String schema, String name,
            boolean ifExists);
}

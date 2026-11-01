package org.pangrova.spi.file.space;

import java.util.Objects;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.common.file.FileSpaceType;

/**
 * Definition used to create a Volume or Stage.
 *
 * @param type             which namespace surface owns this space
 * @param catalog          catalog name
 * @param schema           schema name
 * @param name             volume or stage name
 * @param external         true for EXTERNAL VOLUME / EXTERNAL STAGE
 * @param storageProfile   named storage profile used to reach the location
 * @param location         external location; empty for managed/internal spaces
 * @param directoryEnabled whether the stage directory projection is enabled
 * @param comment          user comment
 * @param owner            owning principal
 */
@Experimental
public record FileSpaceDefinition(
        FileSpaceType type,
        String catalog,
        String schema,
        String name,
        boolean external,
        String storageProfile,
        String location,
        boolean directoryEnabled,
        String comment,
        String owner
) {

    public FileSpaceDefinition {
        Objects.requireNonNull(type, "type must not be null");
        catalog = requireName(catalog, "catalog");
        schema = requireName(schema, "schema");
        name = requireName(name, "name");
        Objects.requireNonNull(owner, "owner must not be null");
        storageProfile = storageProfile == null ? "" : storageProfile.trim();
        location = location == null ? "" : location.trim();
        comment = comment == null ? "" : comment;
    }

    private static String requireName(String value, String label) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(label + " must not be blank");
        }
        return value;
    }
}

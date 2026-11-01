package org.pangrova.spi.file.space;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.common.file.FileSpaceType;

/**
 * Durable state of a Volume or Stage.
 *
 * <p>This is an internal registry row, not a user-visible asset or version.
 * {@code id} is a namespace incarnation retained through rename and never reused after drop.
 * File identity lives on the referenced objects (path plus provider
 * version/ETag/checksum), never on this record. {@code managedRoot} is the
 * persistent profile-relative namespace prefix, retained across renames and
 * empty for external spaces.</p>
 */
@Experimental
public record FileSpace(
        UUID id,
        FileSpaceType type,
        String catalog,
        String schema,
        String name,
        boolean external,
        String storageProfile,
        String location,
        String managedRoot,
        boolean directoryEnabled,
        String comment,
        String owner,
        Instant createdAt,
        Instant updatedAt
) {

    public FileSpace {
        Objects.requireNonNull(id, "id must not be null");
        Objects.requireNonNull(type, "type must not be null");
        Objects.requireNonNull(catalog, "catalog must not be null");
        Objects.requireNonNull(schema, "schema must not be null");
        Objects.requireNonNull(name, "name must not be null");
        Objects.requireNonNull(owner, "owner must not be null");
        Objects.requireNonNull(createdAt, "createdAt must not be null");
        Objects.requireNonNull(updatedAt, "updatedAt must not be null");
        storageProfile = storageProfile == null ? "" : storageProfile;
        location = location == null ? "" : location;
        Objects.requireNonNull(managedRoot, "managedRoot must not be null");
        if (external ? !managedRoot.isEmpty() : managedRoot.isBlank()) {
            throw new IllegalArgumentException("Only managed spaces must have a persistent storage root");
        }
        comment = comment == null ? "" : comment;
    }

    public String qualifiedName() {
        return catalog + "." + schema + "." + name;
    }
}

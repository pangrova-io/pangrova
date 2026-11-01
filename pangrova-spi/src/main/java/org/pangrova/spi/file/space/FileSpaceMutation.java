package org.pangrova.spi.file.space;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Single-field changes applied by {@code ALTER VOLUME} and {@code ALTER STAGE}.
 *
 * <p>Each mutation is applied inside the registry transaction so that
 * read-modify-write never loses a concurrent change.</p>
 */
@Experimental
public sealed interface FileSpaceMutation {

    /** Point the space at a different storage profile. */
    record SetStorageProfile(String storageProfile) implements FileSpaceMutation {
        public SetStorageProfile {
            if (storageProfile == null || storageProfile.isBlank()) {
                throw new IllegalArgumentException("storageProfile must not be blank");
            }
        }
    }

    /** Change the external location (Stage URL or external Volume location). */
    record SetLocation(String location) implements FileSpaceMutation {
        public SetLocation {
            if (location == null || location.isBlank()) {
                throw new IllegalArgumentException("location must not be blank");
            }
        }
    }

    /** Enable or disable the stage directory projection. */
    record SetDirectoryEnabled(boolean enabled) implements FileSpaceMutation {}

    /** Replace the comment; empty removes it. */
    record SetComment(String comment) implements FileSpaceMutation {
        public SetComment {
            comment = comment == null ? "" : comment;
        }
    }

    /** Transfer ownership. */
    record SetOwner(String owner) implements FileSpaceMutation {
        public SetOwner {
            if (owner == null || owner.isBlank()) {
                throw new IllegalArgumentException("owner must not be blank");
            }
        }
    }
}

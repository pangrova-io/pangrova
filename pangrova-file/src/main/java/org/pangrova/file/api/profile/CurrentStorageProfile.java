package org.pangrova.file.api.profile;

import org.pangrova.common.api.annotation.Experimental;

import java.util.Optional;

/** Reads the current immutable profile definition for File authorization. */
@Experimental
public interface CurrentStorageProfile {
    /** Returns the exact current immutable definition and its durable identity. */
    Optional<StorageProfileRevision> getCurrentRevision(String name);
}

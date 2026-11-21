package org.pangrova.filesystem.api.object;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.common.enums.StorageType;
import org.pangrova.filesystem.api.profile.StorageProfile;

/** Owns one backend's accessor cache and provider resources, without exposing SDK types. */
@Experimental
public interface ObjectStoreAccessorProvider extends AutoCloseable {
    /** Returns the only backend accepted by this provider. */
    StorageType storageType();

    /** Resolves the complete immutable profile while the provider is open; the returned accessor is borrowed. */
    ObjectStoreAccessor accessor(StorageProfile profile);

    /**
     * Rejects new allocations and closes owned resources, preserving failure on repeated calls.
     * The host retires active reads before closing their provider.
     */
    @Override
    void close();
}

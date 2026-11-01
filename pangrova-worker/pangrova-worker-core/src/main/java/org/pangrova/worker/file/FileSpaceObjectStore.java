package org.pangrova.worker.file;

import java.util.Objects;
import java.util.Optional;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.filesystem.api.profile.StorageProfile;
import org.pangrova.persistence.api.file.profile.StorageProfileRegistry;
import org.pangrova.spi.file.space.FileSpace;
import org.pangrova.spi.file.space.FileSpaceLayout;
import org.pangrova.filesystem.api.object.ObjectLocation;
import org.pangrova.filesystem.api.object.ObjectStoreAccessor;

/**
 * Maps a Volume or Stage to a concrete object-store root and backend.
 *
 * <p>External spaces must resolve through a registered profile whose prefix
 * contains the declared location; managed spaces use the persistent registry
 * root within their profile. Resolution fails closed when no
 * profile is configured; it never falls back to the process default bucket.</p>
 */
public final class FileSpaceObjectStore implements AutoCloseable {

    private final StorageProfileRegistry profiles;
    private final String managedProfileName;
    private final ObjectStoreAccessorFactory accessors;

    public FileSpaceObjectStore(StorageProfileRegistry profiles, String managedProfileName) {
        this(profiles, managedProfileName, new ObjectStoreAccessorFactory());
    }

    public FileSpaceObjectStore(StorageProfileRegistry profiles, String managedProfileName,
            ObjectStoreAccessorFactory accessors) {
        this.profiles = Objects.requireNonNull(profiles, "profiles must not be null");
        this.managedProfileName = managedProfileName == null ? "" : managedProfileName.trim();
        this.accessors = Objects.requireNonNull(accessors, "accessors must not be null");
    }

    /** Resolves the space root, failing closed when storage is not configured. */
    public ObjectLocation locate(FileSpace space) {
        Objects.requireNonNull(space, "space must not be null");
        StorageProfile profile = profileFor(space);
        String baseKey = space.external()
                ? FileSpaceLayout.externalBaseKey(space, profile)
                : FileSpaceLayout.managedKey(space, profile);
        return new ObjectLocation(profile, baseKey);
    }

    /** Returns the accessor serving the given space. */
    public ObjectStoreAccessor accessor(FileSpace space) {
        return accessors.accessor(locate(space).profile());
    }

    /** Returns the accessor serving a resolved profile. */
    public ObjectStoreAccessor accessor(StorageProfile profile) {
        return accessors.accessor(profile);
    }

    private StorageProfile profileFor(FileSpace space) {
        String name = space.storageProfile().isBlank() ? managedProfileName
                : space.storageProfile();
        if (name.isEmpty()) {
            throw notConfigured(space, "no storage profile is configured");
        }
        Optional<StorageProfile> profile = profiles.get(name);
        if (profile.isEmpty()) {
            throw notConfigured(space, "storage profile '" + name + "' does not exist");
        }
        return profile.get();
    }

    private static PangrovaException notConfigured(FileSpace space, String detail) {
        return new PangrovaException(PangrovaErrorCode.FILE_STORAGE_NOT_CONFIGURED,
                "Storage is not configured for " + space.qualifiedName() + ": " + detail);
    }

    @Override
    public void close() {
        accessors.close();
    }
}

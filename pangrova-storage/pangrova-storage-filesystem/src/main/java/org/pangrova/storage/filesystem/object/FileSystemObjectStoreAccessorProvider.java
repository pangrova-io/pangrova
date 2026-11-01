package org.pangrova.storage.filesystem.object;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.common.storage.StorageType;
import org.pangrova.file.api.object.ObjectStoreAccessor;
import org.pangrova.file.api.object.ObjectStoreAccessorProvider;
import org.pangrova.file.api.profile.StorageProfile;

/** Caches filesystem accessors without introducing any cloud credential provider or SDK. */
public final class FileSystemObjectStoreAccessorProvider implements ObjectStoreAccessorProvider {
    private final Map<StorageProfile, ObjectStoreAccessor> accessors = new HashMap<>();
    private boolean closed;

    @Override
    public StorageType storageType() {
        return StorageType.LOCAL;
    }

    @Override
    public synchronized ObjectStoreAccessor accessor(StorageProfile profile) {
        if (closed) throw new IllegalStateException("Filesystem accessor provider is closed");
        if (profile.type() != StorageType.LOCAL) {
            throw new PangrovaException(PangrovaErrorCode.FILE_STORAGE_UNAVAILABLE,
                    "Filesystem accessor provider cannot serve " + profile.type());
        }
        return accessors.computeIfAbsent(profile, key -> new LocalObjectStoreAccessor(Path.of(key.bucket())));
    }

    @Override
    public synchronized void close() {
        closed = true;
        accessors.clear();
    }
}

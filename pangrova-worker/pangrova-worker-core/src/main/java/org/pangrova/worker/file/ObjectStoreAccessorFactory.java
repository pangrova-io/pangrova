package org.pangrova.worker.file;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.common.storage.StorageType;
import org.pangrova.filesystem.api.object.ObjectStoreAccessor;
import org.pangrova.filesystem.api.object.ObjectStoreAccessorProvider;
import org.pangrova.filesystem.api.profile.StorageProfile;
import org.pangrova.filesystem.local.object.LocalObjectStoreAccessorProvider;
import org.pangrova.filesystem.s3.object.S3ObjectStoreAccessorProvider;

/** Selects one backend provider and owns its lifecycle for a Worker file endpoint. */
public final class ObjectStoreAccessorFactory implements AutoCloseable {
    private final List<ObjectStoreAccessorProvider> owned;
    private final Map<StorageType, ObjectStoreAccessorProvider> providers;
    private final ReentrantReadWriteLock lifecycle = new ReentrantReadWriteLock();
    private final CompletableFuture<Void> closeComplete = new CompletableFuture<>();
    private boolean closed;

    /** Installs independent LOCAL and S3 providers; each owns its own resources. */
    public ObjectStoreAccessorFactory() {
        this(List.of(new LocalObjectStoreAccessorProvider(), new S3ObjectStoreAccessorProvider()));
    }

    /** Takes ownership of providers after validating the complete unique backend inventory. */
    public ObjectStoreAccessorFactory(List<? extends ObjectStoreAccessorProvider> providers) {
        this.owned = List.copyOf(providers);
        var inventory = new EnumMap<StorageType, ObjectStoreAccessorProvider>(StorageType.class);
        for (var provider : owned) {
            var type = Objects.requireNonNull(provider.storageType(), "provider.storageType");
            if (inventory.putIfAbsent(type, provider) != null) {
                throw new IllegalArgumentException("Duplicate object store provider for " + type);
            }
        }
        this.providers = Map.copyOf(inventory);
    }

    /** Resolves a borrowed accessor without admitting allocation after endpoint shutdown. */
    public ObjectStoreAccessor accessor(StorageProfile profile) {
        Objects.requireNonNull(profile, "profile");
        var lock = lifecycle.readLock();
        lock.lock();
        try {
            if (closed) throw new IllegalStateException("Object store accessor factory is closed");
            var provider = providers.get(profile.type());
            if (provider == null) {
                throw new PangrovaException(PangrovaErrorCode.FILE_STORAGE_UNAVAILABLE,
                        "Storage type " + profile.type() + " is not installed for file spaces");
            }
            return provider.accessor(profile);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() {
        boolean retire;
        var lock = lifecycle.writeLock();
        lock.lock();
        try {
            retire = !closed;
            closed = true;
        } finally {
            lock.unlock();
        }
        if (retire) {
            try {
                RuntimeException failure = closeOwned(new java.util.ArrayList<>(owned));
                if (failure == null) closeComplete.complete(null);
                else closeComplete.completeExceptionally(failure);
            } catch (Error failure) {
                closeComplete.completeExceptionally(failure);
            }
        }
        try {
            closeComplete.join();
        } catch (java.util.concurrent.CompletionException failure) {
            if (failure.getCause() instanceof RuntimeException cause) throw cause;
            if (failure.getCause() instanceof Error cause) throw cause;
            throw new IllegalStateException("Object store resource cleanup failed", failure.getCause());
        }
    }

    static RuntimeException closeOwned(java.util.List<AutoCloseable> owned) {
        RuntimeException failure = null;
        Error fatal = null;
        for (var resource : owned) {
            try {
                resource.close();
            } catch (Exception closeFailure) {
                if (failure == null) failure = new IllegalStateException("Object store resource cleanup failed");
                failure.addSuppressed(closeFailure);
            } catch (Error closeFailure) {
                if (fatal == null) fatal = closeFailure;
                else if (fatal != closeFailure) {
                    fatal.addSuppressed(closeFailure);
                }
            }
        }
        if (fatal != null) {
            if (failure != null) fatal.addSuppressed(failure);
            throw fatal;
        }
        return failure;
    }
}

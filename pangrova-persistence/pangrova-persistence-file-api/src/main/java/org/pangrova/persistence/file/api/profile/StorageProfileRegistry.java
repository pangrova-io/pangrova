package org.pangrova.persistence.file.api.profile;

import org.pangrova.filesystem.api.profile.StorageProfile;

import org.pangrova.common.api.annotation.Experimental;

import java.util.List;
import java.util.Optional;

/**
 * Global registry for external storage profiles. Production definitions created
 * through DDL are held in the shared metadata authority.
 *
 * <p>Thread-safe: implementations must support concurrent reads and writes.</p>
 */
@Experimental
public interface StorageProfileRegistry {

    /**
     * Legacy test/configuration registration. Durable authorities reject replacement;
     * SQL creation must use {@link #createIfAbsent(StorageProfile)}.
     */
    void register(StorageProfile profile);

    /** Atomically creates a name if absent; never overwrites an existing definition. */
    boolean createIfAbsent(StorageProfile profile);

    /**
     * Unregisters the storage profile with the given name.
     *
     * @param name profile name to remove
     * @return true if a profile was removed, false if no profile existed with that name
     */
    boolean unregister(String name);

    /**
     * Retrieves a storage profile by name.
     *
     * @param name profile name
     * @return the profile if found
     */
    Optional<StorageProfile> get(String name);

    /** Returns the exact current immutable definition and its durable identity. */
    Optional<StorageProfileRevision> getCurrentRevision(String name);

    /** Returns an exact historical definition; missing revisions must never rebind to current. */
    Optional<StorageProfileRevision> findRevision(String name, String incarnationId, long revision);

    /**
     * Returns all registered storage profiles.
     */
    List<StorageProfile> list();

    /**
     * Resolves the best-matching storage profile for a given URL by matching
     * the URL's bucket and prefix against registered profiles. Only ties at the
     * longest matching prefix are ambiguous; shorter ties cannot hide a unique
     * longer match. Selection must not depend on profile name or iteration order.
     * This lookup does not replace current authorization at the point of use.
     *
     * @throws org.pangrova.file.api.error.FileStorageException when multiple profiles
     *         share the longest matching prefix; never choose an arbitrary profile
     * @param url the object storage URL (e.g. {@code s3://bucket/key})
     * @return the matching profile with the longest prefix match, or empty if none match
     */
    Optional<StorageProfile> resolveByUrl(String url);
}

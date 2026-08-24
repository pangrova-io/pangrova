package org.pangrova.spi.storage;

import org.pangrova.spi.annotation.Experimental;

import java.util.List;
import java.util.Optional;

/**
 * Global registry for external storage profiles.
 * Profiles can be registered via DDL ({@code CREATE STORAGE PROFILE}) or YAML configuration.
 *
 * <p>Thread-safe: implementations must support concurrent reads and writes.</p>
 */
@Experimental
public interface StorageProfileRegistry {

    /**
     * Registers a new storage profile. Replaces any existing profile with the same name.
     *
     * @param profile the profile to register
     */
    void register(StorageProfile profile);

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

    /**
     * Returns all registered storage profiles.
     */
    List<StorageProfile> list();

    /**
     * Resolves the best-matching storage profile for a given URL by matching
     * the URL's bucket and prefix against registered profiles.
     *
     * @param url the object storage URL (e.g. {@code s3://bucket/key})
     * @return the matching profile with the longest prefix match, or empty if none match
     */
    Optional<StorageProfile> resolveByUrl(String url);
}

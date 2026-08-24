package org.pangrova.metadata.mv;

import java.util.List;
import java.util.Optional;

/**
 * Persistent store for materialized view metadata.
 * Implementations must be thread-safe.
 */
public interface MaterializedViewStore {

    /**
     * Save or update MV metadata. Upsert semantics: if an MV with the same name
     * exists, it is replaced.
     */
    void save(MaterializedViewMetadata metadata);

    /**
     * Load MV metadata by name.
     *
     * @return the metadata, or empty if not found
     */
    Optional<MaterializedViewMetadata> load(String mvName);

    /**
     * Delete MV metadata by name.
     *
     * @return true if the entry existed and was removed
     */
    boolean delete(String mvName);

    /**
     * List all non-DROPPED materialized view metadata entries.
     */
    List<MaterializedViewMetadata> listActive();

    /**
     * List all materialized view metadata entries regardless of state.
     */
    List<MaterializedViewMetadata> listAll();
}

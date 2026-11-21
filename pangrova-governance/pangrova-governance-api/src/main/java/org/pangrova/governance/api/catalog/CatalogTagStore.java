package org.pangrova.governance.api.catalog;

import org.pangrova.common.api.annotation.Experimental;

import org.pangrova.governance.api.classification.ColumnTagStore;

import java.util.List;
import java.util.Map;

/**
 * Persistence contract for catalog-level governance tags (e.g. "classification"="pii",
 * "environment"="production", "tier"="critical").
 *
 * <p>G-META-06: catalog-level tags drive governance rules — for example, a PII-tagged
 * catalog may require enhanced audit logging, and a "production"-tagged catalog may
 * enforce stricter access controls. Implementations must be thread-safe.</p>
 *
 * <p>Design mirrors the column-level {@link ColumnTagStore} to maintain API consistency
 * across the governance tag hierarchy (catalog/schema/table/column).</p>
 */
@Experimental
public interface CatalogTagStore {

    /**
     * Persists a tag assignment for a catalog.
     *
     * @param catalogName catalog name
     * @param tagKey      tag key (e.g. "classification", "environment")
     * @param tagValue    tag value (e.g. "pii", "production"); must not be null
     */
    void saveTag(String catalogName, String tagKey, String tagValue);

    /**
     * Removes a specific tag from a catalog.
     *
     * @param catalogName catalog name
     * @param tagKey      tag key to remove
     */
    void deleteTag(String catalogName, String tagKey);

    /**
     * Returns all tags for a specific catalog.
     *
     * @param catalogName catalog name
     * @return map of tagKey to tagValue (never null, may be empty)
     */
    Map<String, String> getTags(String catalogName);

    /**
     * Returns all catalog tag assignments in the store. Used at startup to
     * replay existing tags through the event system for governance activation.
     *
     * @return list of all catalog tag entries (never null, may be empty)
     */
    List<CatalogTagEntry> getAllTags();

    /**
     * A single catalog tag assignment.
     */
    record CatalogTagEntry(
            String catalogName,
            String tagKey,
            String tagValue
    ) {}
}

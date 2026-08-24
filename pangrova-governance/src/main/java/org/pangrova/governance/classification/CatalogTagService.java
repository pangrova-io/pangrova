package org.pangrova.governance.classification;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.pangrova.common.util.Preconditions.checkNotNull;

/**
 * Manages catalog-level governance tags and notifies listeners when tags change.
 *
 * <p>G-META-06: catalog-level tags enable classification-driven governance. Tags
 * such as "classification=pii" or "environment=production" can trigger governance
 * rules (e.g., enhanced audit logging for PII catalogs, read-only enforcement for
 * production catalogs).</p>
 *
 * <p>Design mirrors {@link ColumnTagService} for consistency across the tag
 * hierarchy. Listeners are notified synchronously on tag changes via
 * {@link CatalogTagChangedListener}.</p>
 */
public class CatalogTagService {

    private static final Logger LOG = LoggerFactory.getLogger(CatalogTagService.class);

    private final CatalogTagStore tagStore;
    private final List<CatalogTagChangedListener> listeners = new CopyOnWriteArrayList<>();

    /**
     * Creates a service backed by the given {@link CatalogTagStore}.
     */
    public CatalogTagService(CatalogTagStore tagStore) {
        this.tagStore = Objects.requireNonNull(tagStore, "tagStore must not be null");
    }

    /**
     * Registers a listener that will be notified on catalog tag changes.
     */
    public void addListener(CatalogTagChangedListener listener) {
        checkNotNull(listener, "listener must not be null");
        listeners.add(listener);
    }

    /**
     * Removes a previously registered listener.
     */
    public void removeListener(CatalogTagChangedListener listener) {
        listeners.remove(listener);
    }

    /**
     * Assigns a tag to a catalog, persists it, and notifies listeners.
     *
     * @param catalogName the catalog name
     * @param tagKey      the tag key (e.g. "classification", "environment")
     * @param tagValue    the tag value (e.g. "pii", "production")
     */
    public void assignTag(String catalogName, String tagKey, String tagValue) {
        checkNotNull(catalogName, "catalogName must not be null");
        checkNotNull(tagKey, "tagKey must not be null");
        checkNotNull(tagValue, "tagValue must not be null");

        String oldValue = tagStore.getTags(catalogName).get(tagKey);
        tagStore.saveTag(catalogName, tagKey, tagValue);

        LOG.info("event=catalog_tag_assigned catalog={} tagKey={} tagValue={}",
                catalogName, tagKey, tagValue);
        dispatchEvent(catalogName, tagKey, oldValue, tagValue);
    }

    /**
     * Removes a tag from a catalog, deletes it from the store, and notifies listeners.
     *
     * @param catalogName the catalog name
     * @param tagKey      the tag key to remove
     */
    public void removeTag(String catalogName, String tagKey) {
        checkNotNull(catalogName, "catalogName must not be null");
        checkNotNull(tagKey, "tagKey must not be null");

        String oldValue = tagStore.getTags(catalogName).get(tagKey);
        if (oldValue == null) {
            LOG.debug("event=catalog_tag_remove_noop catalog={} tagKey={} reason=not_present",
                    catalogName, tagKey);
            return;
        }
        tagStore.deleteTag(catalogName, tagKey);

        LOG.info("event=catalog_tag_removed catalog={} tagKey={} oldValue={}",
                catalogName, tagKey, oldValue);
        dispatchEvent(catalogName, tagKey, oldValue, null);
    }

    /**
     * Returns all tags currently assigned to a catalog.
     *
     * @param catalogName the catalog name
     * @return map of tagKey to tagValue (never null, may be empty)
     */
    public Map<String, String> getTags(String catalogName) {
        checkNotNull(catalogName, "catalogName must not be null");
        return tagStore.getTags(catalogName);
    }

    /**
     * Checks whether a catalog has a specific tag with the given value.
     *
     * @param catalogName the catalog name
     * @param tagKey      the tag key
     * @param tagValue    the expected tag value
     * @return true if the catalog has the tag with the specified value
     */
    public boolean hasTag(String catalogName, String tagKey, String tagValue) {
        checkNotNull(catalogName, "catalogName must not be null");
        checkNotNull(tagKey, "tagKey must not be null");
        checkNotNull(tagValue, "tagValue must not be null");
        return tagValue.equals(tagStore.getTags(catalogName).get(tagKey));
    }

    /**
     * Replays all persisted catalog tags through the event pipeline so listeners
     * can rebuild their derived state on coordinator startup.
     *
     * @return number of catalog tags replayed
     */
    public int loadExistingTags() {
        List<CatalogTagStore.CatalogTagEntry> entries = tagStore.getAllTags();
        for (CatalogTagStore.CatalogTagEntry e : entries) {
            dispatchEvent(e.catalogName(), e.tagKey(), null, e.tagValue());
        }
        LOG.info("event=catalog_tags_replayed count={}", entries.size());
        return entries.size();
    }

    private void dispatchEvent(String catalogName, String tagKey,
                               String oldValue, String newValue) {
        for (CatalogTagChangedListener listener : listeners) {
            try {
                listener.onCatalogTagChanged(catalogName, tagKey, oldValue, newValue);
            } catch (Exception e) {
                LOG.error("event=catalog_tag_listener_failed listenerClass={} catalog={} tagKey={}",
                        listener.getClass().getSimpleName(), catalogName, tagKey, e);
            }
        }
    }

    /**
     * Listener interface for catalog tag change events.
     */
    @FunctionalInterface
    public interface CatalogTagChangedListener {
        /**
         * Called when a catalog tag is assigned, changed, or removed.
         *
         * @param catalogName the catalog name
         * @param tagKey      the tag key
         * @param oldValue    the previous value, or null if newly assigned
         * @param newValue    the new value, or null if removed
         */
        void onCatalogTagChanged(String catalogName, String tagKey,
                                 String oldValue, String newValue);
    }
}

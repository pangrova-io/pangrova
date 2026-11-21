package org.pangrova.governance.api.classification;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Receives notifications when column or table tags are changed.
 */
@FunctionalInterface
@Experimental
public interface TagChangedEventListener {

    /**
     * Called when a tag is assigned, updated, or removed.
     *
     * @param event details of the tag change
     */
    void onTagChanged(TagChangedEvent event);
}

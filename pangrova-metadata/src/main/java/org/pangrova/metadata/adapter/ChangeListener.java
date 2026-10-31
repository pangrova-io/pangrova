package org.pangrova.metadata.adapter;

import org.pangrova.metadata.api.catalog.MetadataChange;

/**
 * Functional interface for push-based metadata change notifications.
 */
@FunctionalInterface
public interface ChangeListener {

    void onChange(MetadataChange change);
}

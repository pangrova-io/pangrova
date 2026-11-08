package org.pangrova.metadata.api.catalog;


/**
 * Functional interface for push-based metadata change notifications.
 */
@FunctionalInterface
public interface ChangeListener {

    void onChange(MetadataChange change);
}

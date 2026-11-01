package org.pangrova.metadata.core.federation;

/**
 * Functional interface for push-based metadata change notifications.
 */
@FunctionalInterface
public interface ChangeListener {

    void onChange(MetadataChange change);
}

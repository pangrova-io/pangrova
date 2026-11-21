package org.pangrova.metadata.api.catalog;

import org.pangrova.common.api.annotation.Experimental;


/**
 * Functional interface for push-based metadata change notifications.
 */
@FunctionalInterface
@Experimental
public interface ChangeListener {

    void onChange(MetadataChange change);
}

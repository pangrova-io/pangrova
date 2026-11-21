package org.pangrova.metadata.api.event;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Functional interface for receiving metadata lifecycle events.
 */
@FunctionalInterface
@Experimental
public interface MetadataEventListener {

    void onEvent(MetadataEvent event);
}

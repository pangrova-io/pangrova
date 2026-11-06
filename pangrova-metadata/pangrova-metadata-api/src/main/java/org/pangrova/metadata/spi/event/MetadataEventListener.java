package org.pangrova.metadata.spi.event;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.metadata.api.event.MetadataEvent;

/**
 * Functional interface for receiving metadata lifecycle events.
 */
@FunctionalInterface
@Experimental
public interface MetadataEventListener {

    void onEvent(MetadataEvent event);
}

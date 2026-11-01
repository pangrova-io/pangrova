package org.pangrova.metadata.event;

/**
 * Functional interface for receiving metadata lifecycle events.
 */
@FunctionalInterface
public interface MetadataEventListener {

    void onEvent(MetadataEvent event);
}

package org.pangrova.materialization.port;

/**
 * Transactional event outbox.
 *
 * <p>Use cases that are not already writing through
 * {@link MaterializationRepository} append events here. The outbox commits
 * with the surrounding transaction; delivery is asynchronous and at-least-once.
 */
public interface EventOutbox {

    /**
     * Appends an event to the outbox.
     *
     * @param event event to append
     */
    void append(MaterializationEvent event);
}

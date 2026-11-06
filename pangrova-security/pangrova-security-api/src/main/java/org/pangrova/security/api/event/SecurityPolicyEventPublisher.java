package org.pangrova.security.api.event;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Publishes a security policy change notification after its authoritative mutation.
 * Implementations must document delivery and failure semantics. This operation alone
 * does not provide durable delivery or an atomic outbox, and cannot authorize a request.
 */
@Experimental
@FunctionalInterface
public interface SecurityPolicyEventPublisher {
    /** Publishes a non-null event; the caller retains ownership of the immutable event. */
    void publish(SecurityPolicyChangeEvent event);
}

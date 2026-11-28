package org.pangrova.security.api.secret;

import org.pangrova.common.api.annotation.Experimental;

import org.pangrova.common.concurrency.ThreadSafe;

/**
 * Resolves one secret reference to its value.
 *
 * <p>Consumers that store references in a URI form may call
 * {@link #resolve(String)}, which parses {@code scheme://key} into a
 * {@link SecretReference}. Secret values must never be logged or embedded in
 * task requests.</p>
 */
@ThreadSafe
@Experimental
public interface SecretResolver {

    /**
     * Resolves one reference.
     *
     * @return the secret value; never null
     * @throws IllegalArgumentException if the reference is malformed or unsupported
     * @throws IllegalStateException    if the backend cannot produce the secret
     */
    String resolve(SecretReference reference);

    /** Resolves a {@code scheme://key} reference. */
    default String resolve(String reference) {
        return resolve(SecretReference.parse(reference));
    }
}

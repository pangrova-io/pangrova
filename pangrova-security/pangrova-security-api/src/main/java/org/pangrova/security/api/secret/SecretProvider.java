package org.pangrova.security.api.secret;

import org.pangrova.security.api.secret.SecretResolver;

import org.pangrova.common.api.annotation.Experimental;

import org.pangrova.common.concurrent.ThreadSafe;

/**
 * Prefix-scoped secret backend contract.
 *
 * <p>Implementations live in their owner artifact under a provider subpackage;
 * selectors see only this interface.</p>
 */
@ThreadSafe
@Experimental
public interface SecretProvider extends SecretResolver {

    /** Provider prefix used in configuration references, for example {@code ENV}. */
    String prefix();
}

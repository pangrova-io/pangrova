package org.pangrova.security.api.secret;

import org.pangrova.common.concurrent.ThreadSafe;

/**
 * Prefix-scoped secret backend contract.
 *
 * <p>Implementations live in their owner artifact under a provider subpackage;
 * selectors see only this interface.</p>
 */
@ThreadSafe
public interface SecretProvider extends SecretResolver {

    /** Provider prefix used in configuration references, for example {@code ENV}. */
    String prefix();
}

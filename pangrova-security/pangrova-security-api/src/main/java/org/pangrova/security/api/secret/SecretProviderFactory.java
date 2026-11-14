package org.pangrova.security.api.secret;

import org.pangrova.common.api.annotation.Experimental;

/** A discoverable prefix owner. Discovery validates all factories before creating any resolvers. */
@Experimental
public interface SecretProviderFactory {
    int API_VERSION = 1;

    /** The compatible contract version compiled into this implementation. */
    int apiVersion();

    String prefix();

    /** Creates a thread-safe resolver for this registry. Resolution borrows no persistent backend resources. */
    SecretProvider create();
}

package org.pangrova.connector.spi;

import org.pangrova.common.api.annotation.Experimental;

/** Worker-local resolver for opaque connector secret references. */
@Experimental
public interface ConnectorSecretResolver {

    /** Provider prefix used in {@code ${PREFIX:reference}} expressions. */
    String prefix();

    /** Resolves a tenant-scoped reference without logging either value. */
    String resolve(String tenantId, String reference);
}

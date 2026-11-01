/**
 * Immutable observations of exact permissions, connector definitions and model identities.
 *
 * <p>These values describe authority admitted earlier; they do not grant current access.
 * Consumers must revalidate them through the Security authority before replaying work.
 * This package has no dependency on policy implementations, persistence or JSON codecs.</p>
 */
package org.pangrova.spi.security.authz;

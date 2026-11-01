/**
 * Secret resolution ports shared by schedule, connector, AI, and security wiring.
 *
 * <p>{@link org.pangrova.spi.security.secret.SecretResolver} resolves one opaque
 * reference; {@link org.pangrova.spi.security.secret.SecretProvider} is the
 * prefix-scoped SPI that backends implement. Provider implementations live in
 * their owner artifact, never in this package.</p>
 */
package org.pangrova.spi.security.secret;

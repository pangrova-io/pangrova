/**
 * REST client implementation: request construction, retry/failover, response
 * mapping, result streaming hand-off, and catalog metadata parsing.
 *
 * <p>No Arrow or Arrow Flight imports: this package depends on the type-erased
 * {@code pangrova.transport} result API.
 */
package org.pangrova.client.transport.http;

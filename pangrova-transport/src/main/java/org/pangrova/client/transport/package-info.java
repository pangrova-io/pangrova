/**
 * Lightweight HTTP/JSON transport for the public client contract.
 *
 * <p>Owns the REST client implementation ({@link org.pangrova.client.transport.http.PangrovaRestClient}),
 * endpoint failover, retry execution, response mapping, and catalog metadata parsing.
 * Arrow and Arrow Flight stay in the sibling {@code org.pangrova.transport} roots;
 * typed credentials live in {@link org.pangrova.client.transport.security}.
 *
 * <p>Public DTOs, exceptions, and session policies belong to {@code pangrova-client-core};
 * server authentication, planning, and execution remain server responsibilities.
 */
package org.pangrova.client.transport;

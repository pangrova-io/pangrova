/**
 * Transport-owned immutable result-channel security policies.
 *
 * <p>Consumed by client configuration and the result transport implementation.
 * This public capability exposes paths and policy only, with redacted diagnostics;
 * it must not expose transport SDK types, interpret Java REST trust stores, own
 * authentication authority, or weaken certificate and hostname verification.</p>
 */
package org.pangrova.transport.security;

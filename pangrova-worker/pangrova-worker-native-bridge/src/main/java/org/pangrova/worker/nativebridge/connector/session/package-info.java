/**
 * Worker-local immutable connector sessions bound to canonical task identity and security.
 * These internal values are shared by catalog binding and connector adapters. They do not
 * own catalog leases, native handles, page sinks or execution lifecycle state.
 */
package org.pangrova.worker.nativebridge.connector.session;

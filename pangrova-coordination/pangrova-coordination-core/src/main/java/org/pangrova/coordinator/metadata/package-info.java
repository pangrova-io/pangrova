/**
 * Coordinator-owned catalog metadata assembly and Native control integration.
 *
 * <p>Internal assembly boundary for Coordinator bootstrap and authenticated query
 * callers. It wires catalog owners and delegates immutable planning read binding
 * to the read capability package. It does not own a second snapshot authority or
 * execute source rows; public APIs have no external compatibility guarantee.</p>
 */
package org.pangrova.coordinator.metadata;

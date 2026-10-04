/**
 * Coordinator-owned query read binding and planning metadata boundary.
 *
 * <p>Internal to Coordinator assembly and authenticated query compilation. Schema,
 * statistics and execution splits retain the same immutable connector source
 * snapshot; the connector handle remains the snapshot authority. This package
 * must not introduce a second pin registry, read rows, or perform writes. Public
 * entry points serve assembly and query callers; schema collaborators remain
 * package-private and have no independent compatibility guarantee.</p>
 */
package org.pangrova.coordinator.metadata.read;

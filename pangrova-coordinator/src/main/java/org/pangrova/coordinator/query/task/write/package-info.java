/**
 * Coordinator-owned write allocation identity. Internal stable values are consumed by scheduling
 * and commit tracking inside this leaf; this package does not authorize writes, perform SDK I/O,
 * execute data, or define a second serialized execution protocol.
 */
package org.pangrova.coordinator.query.task.write;

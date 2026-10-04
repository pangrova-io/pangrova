/**
 * Transport-owned result streaming, columnar serialization and client lifecycle.
 *
 * <p>Consumed by client modules through framework-isolated result types and the
 * transport implementation. External SDK types remain in this owner artifact.
 * The package must not authorize SQL, invent terminal task or publication states,
 * or replace the canonical protocol and signed result endpoint authority.</p>
 */
package org.pangrova.transport;

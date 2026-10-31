/**
 * Canonical Worker execution identity shared by task admission and REST envelopes.
 * JSON field names are transport projections; process allocation and persistence belong to their owners.
 *
 * <p>Owned by pangrova-protocol; consumed by engine, connector and control-plane adapters.
 * Handwritten Java APIs move with the repository; canonical wire compatibility is governed separately.</p>
 */
package org.pangrova.protocol.execution.identity;
